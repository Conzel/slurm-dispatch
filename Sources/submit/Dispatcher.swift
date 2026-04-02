import Foundation

enum DispatchError: Error, LocalizedError {
    case noClustersAvailable
    case sshFailed(host: String, command: String, output: String)
    case scpFailed(file: String, output: String)
    case sbatchOutputUnexpected(String)

    var errorDescription: String? {
        switch self {
        case .noClustersAvailable:
            return "No clusters are reachable. Check SSH connectivity and config.json."
        case .sshFailed(let host, let cmd, let output):
            return "SSH command failed on \(host): \(cmd)\n\(output)"
        case .scpFailed(let file, let output):
            return "SCP of '\(file)' failed:\n\(output)"
        case .sbatchOutputUnexpected(let output):
            return "Could not parse SLURM job ID from sbatch output: \(output)"
        }
    }
}

// MARK: - Setup

func setupAllClusters(config: Config) throws {
    let s3ImagePath = config.s3.singularityImagePath
    let s3HashPath: String
    if let dotIndex = s3ImagePath.lastIndex(of: ".") {
        s3HashPath = String(s3ImagePath[..<dotIndex]) + ".md5"
    } else {
        s3HashPath = s3ImagePath + ".md5"
    }

    for (name, cluster) in config.clusters {
        print("Setting up \(name) (\(cluster.host))...")

        let repoDir = cluster.remoteRepoDir

        // Clone or update the repo
        try setupRepo(cluster: cluster, config: config)

        let s3cfgRemote = "\(repoDir)/.s3cfg"
        print("  Staging .s3cfg...")
        let scpResult = try scp(localPath: config.s3.expandedS3cfgPath, cluster: cluster, remotePath: s3cfgRemote)
        guard scpResult.succeeded else {
            throw DispatchError.scpFailed(file: ".s3cfg", output: scpResult.combinedOutput)
        }
        _ = try ssh(cluster: cluster, command: "chmod 600 \(s3cfgRemote)")

        // Download singularity image (force)
        let imageRemote = "\(repoDir)/container.sif"
        print("  Downloading Singularity image (force)...")
        let downloadResult = try ssh(
            cluster: cluster,
            command: "S3CMD_CONFIG=\(s3cfgRemote) s3cmd get --force \(s3ImagePath) \(imageRemote)"
        )
        guard downloadResult.succeeded else {
            throw DispatchError.sshFailed(
                host: cluster.host,
                command: "s3cmd get container.sif",
                output: downloadResult.combinedOutput
            )
        }
        print("  Singularity image downloaded.")

        // Verify MD5
        print("  Verifying MD5...")
        let md5Tmp = "\(repoDir)/container.md5"
        let md5Download = try ssh(
            cluster: cluster,
            command: "S3CMD_CONFIG=\(s3cfgRemote) s3cmd get --force \(s3HashPath) \(md5Tmp)"
        )
        let remoteHashResult = try ssh(cluster: cluster, command: "awk '{print $1}' \(md5Tmp)")
        let localHashResult = try ssh(cluster: cluster, command: "md5sum \(imageRemote) | awk '{print $1}'")

        let remoteHash = remoteHashResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        let localHash = localHashResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines)

        if md5Download.succeeded && remoteHashResult.succeeded && !remoteHash.isEmpty {
            if localHash == remoteHash {
                print("  MD5 verified: \(localHash)")
            } else {
                fputs("  WARNING: MD5 mismatch on \(name)! local=\(localHash) remote=\(remoteHash)\n", stderr)
            }
        } else {
            print("  No remote .md5 found at \(s3HashPath), skipping verification.")
        }

        print("  \(name) setup complete.")
    }
}

// MARK: - Load balancing

func getExpectedStartTime(cluster: ClusterConfig, scriptPath: String) throws -> Date {
    let parentDir = (cluster.remoteRepoDir as NSString).deletingLastPathComponent
    let remoteTmp = "\(parentDir)/.submit_test_\(UUID().uuidString).sbatch"
    defer { _ = try? ssh(cluster: cluster, command: "rm -f \(remoteTmp)") }

    let scpResult = try scp(localPath: scriptPath, cluster: cluster, remotePath: remoteTmp)
    guard scpResult.succeeded else {
        throw DispatchError.scpFailed(file: scriptPath, output: scpResult.combinedOutput)
    }

    let result = try ssh(cluster: cluster, command: "sbatch --test-only \(remoteTmp) 2>&1")
    // sbatch --test-only outputs to stderr, e.g.:
    // "sbatch: Job 12345 to start at 2024-01-15T10:30:00 using 4 processors on nodes node01"
    let output = result.combinedOutput
    guard let range = output.range(of: "start at ") else {
        throw DispatchError.sbatchOutputUnexpected(output)
    }
    let afterStartAt = output[range.upperBound...]
    let dateString = String(afterStartAt.prefix(while: { !$0.isWhitespace }))

    let formatter = DateFormatter()
    formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
    formatter.locale = Locale(identifier: "en_US_POSIX")

    guard let date = formatter.date(from: dateString) else {
        throw DispatchError.sbatchOutputUnexpected(output)
    }
    return date
}

func selectCluster(from clusters: [String: ClusterConfig], scriptPath: String) throws -> (name: String, config: ClusterConfig) {
    let timeFormatter = DateFormatter()
    timeFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss"

    var best: (name: String, config: ClusterConfig, startTime: Date)?

    for (name, cluster) in clusters {
        do {
            let startTime = try getExpectedStartTime(cluster: cluster, scriptPath: scriptPath)
            print("  \(name) (\(cluster.host)): expected start at \(timeFormatter.string(from: startTime))")
            if best == nil || startTime < best!.startTime {
                best = (name, cluster, startTime)
            }
        } catch {
            print("  \(name) (\(cluster.host)): unreachable — \(error.localizedDescription)")
        }
    }

    guard let selected = best else {
        throw DispatchError.noClustersAvailable
    }
    return (selected.name, selected.config)
}

// MARK: - Cache validation

func validateCache(cluster: ClusterConfig, config: Config) throws {
    let repoDir = cluster.remoteRepoDir
    let s3cfgRemote = "\(repoDir)/.s3cfg"
    let imageRemote = "\(repoDir)/container.sif"

    // Stage .s3cfg if missing
    let s3cfgCheck = try ssh(cluster: cluster, command: "test -f \(s3cfgRemote) && echo exists || echo missing")
    if s3cfgCheck.stdout.contains("missing") {
        print("  Staging .s3cfg...")
        let result = try scp(localPath: config.s3.expandedS3cfgPath, cluster: cluster, remotePath: s3cfgRemote)
        guard result.succeeded else {
            throw DispatchError.scpFailed(file: ".s3cfg", output: result.combinedOutput)
        }
        _ = try ssh(cluster: cluster, command: "chmod 600 \(s3cfgRemote)")
    } else {
        print("  .s3cfg already present.")
    }

    // Stage HF token if configured
    if let localHfToken = config.tokens?.expandedHfTokenPath {
        let hfTokenRemote = "\(repoDir)/.hf_token"
        let hfCheck = try ssh(cluster: cluster, command: "test -f \(hfTokenRemote) && echo exists || echo missing")
        if hfCheck.stdout.contains("missing") {
            print("  Staging HF token...")
            let result = try scp(localPath: localHfToken, cluster: cluster, remotePath: hfTokenRemote)
            guard result.succeeded else {
                throw DispatchError.scpFailed(file: ".hf_token", output: result.combinedOutput)
            }
            _ = try ssh(cluster: cluster, command: "chmod 600 \(hfTokenRemote)")
        } else {
            print("  HF token already present.")
        }
    }

    // Check Singularity image is present (use --setup to download)
    let sifCheck = try ssh(cluster: cluster, command: "test -f \(imageRemote) && echo exists || echo missing")
    if sifCheck.stdout.contains("missing") {
        throw DispatchError.sshFailed(
            host: cluster.host,
            command: "test -f container.sif",
            output: "Singularity image not found at \(imageRemote). Run 'submit --setup' first."
        )
    }
    print("  Singularity image present.")
}

// MARK: - Job dispatch

func setupRepo(cluster: ClusterConfig, config: Config) throws {
    let repoDir = cluster.remoteRepoDir
    let gitSetup = """
    if [ -d \(repoDir)/.git ]; then \
      cd \(repoDir) && git checkout \(config.git.remoteBranch) && git pull; \
    else \
      git clone --branch \(config.git.remoteBranch) \(config.git.remoteUrl) \(repoDir); \
    fi
    """
    print("Setting up remote repo...")
    let gitResult = try ssh(cluster: cluster, command: gitSetup, agentForward: true)
    if !gitResult.combinedOutput.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
        for line in gitResult.combinedOutput.components(separatedBy: .newlines) {
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmed.isEmpty { print("  \(trimmed)") }
        }
    }
    guard gitResult.succeeded else {
        throw DispatchError.sshFailed(host: cluster.host, command: "git clone/pull", output: gitResult.combinedOutput)
    }
}

func dispatchJob(
    cluster: ClusterConfig,
    config: Config,
    scriptPath: String
) throws -> String {
    let repoDir = cluster.remoteRepoDir
    let scriptName = URL(fileURLWithPath: scriptPath).lastPathComponent
    let userScriptContent = try String(contentsOfFile: scriptPath, encoding: .utf8)

    // Generate wrapper script
    let wrapperContent = generateWrapper(
        cluster: cluster,
        config: config,
        repoDir: repoDir,
        scriptName: scriptName,
        userScriptContent: userScriptContent
    )

    // Write wrapper to a local temp file
    let tmpWrapper = FileManager.default.temporaryDirectory
        .appendingPathComponent("submit_wrapper_\(UUID().uuidString).sh")
    try wrapperContent.write(to: tmpWrapper, atomically: true, encoding: .utf8)
    defer { try? FileManager.default.removeItem(at: tmpWrapper) }

    // SCP user script into repo root
    let scpScript = try scp(localPath: scriptPath, cluster: cluster, remotePath: "\(repoDir)/\(scriptName)")
    guard scpScript.succeeded else {
        throw DispatchError.scpFailed(file: scriptName, output: scpScript.combinedOutput)
    }

    // Ensure logs directory exists before SCP-ing the wrapper into it
    let mklogsResult = try ssh(cluster: cluster, command: "mkdir -p \(repoDir)/logs")
    guard mklogsResult.succeeded else {
        throw DispatchError.sshFailed(host: cluster.host, command: "mkdir -p \(repoDir)/logs", output: mklogsResult.combinedOutput)
    }

    // SCP wrapper into logs/ with a temporary name
    let tmpWrapperName = "wrapper_tmp_\(UUID().uuidString).sh"
    let scpWrapper = try scp(localPath: tmpWrapper.path, cluster: cluster, remotePath: "\(repoDir)/logs/\(tmpWrapperName)")
    guard scpWrapper.succeeded else {
        throw DispatchError.scpFailed(file: tmpWrapperName, output: scpWrapper.combinedOutput)
    }

    // Submit via sbatch from repo root
    let sbatchResult = try ssh(
        cluster: cluster,
        command: "cd \(repoDir) && sbatch logs/\(tmpWrapperName)"
    )
    guard sbatchResult.succeeded else {
        throw DispatchError.sshFailed(host: cluster.host, command: "sbatch \(tmpWrapperName)", output: sbatchResult.combinedOutput)
    }

    // Parse "Submitted batch job <ID>" from sbatch output
    let output = sbatchResult.stdout
    guard let slurmId = output.components(separatedBy: .whitespaces).last, !slurmId.isEmpty else {
        throw DispatchError.sbatchOutputUnexpected(output)
    }

    // Rename wrapper to use the Slurm job ID
    _ = try ssh(cluster: cluster, command: "mv \(repoDir)/logs/\(tmpWrapperName) \(repoDir)/logs/wrapper_\(slurmId).sh")

    return slurmId
}
