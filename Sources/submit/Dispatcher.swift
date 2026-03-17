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

// MARK: - Load balancing

func countRunningJobs(cluster: ClusterConfig) throws -> Int {
    let result = try ssh(
        cluster: cluster,
        command: "squeue -r --me --noheader 2>/dev/null | wc -l"
    )
    guard result.succeeded else {
        throw DispatchError.sshFailed(host: cluster.host, command: "squeue", output: result.combinedOutput)
    }
    return Int(result.stdout) ?? 0
}

func selectCluster(from clusters: [String: ClusterConfig]) throws -> (name: String, config: ClusterConfig) {
    var best: (name: String, config: ClusterConfig, count: Int)?

    for (name, cluster) in clusters {
        do {
            let count = try countRunningJobs(cluster: cluster)
            print("  \(name) (\(cluster.host)): \(count) queued/running jobs")
            if best == nil || count < best!.count {
                best = (name, cluster, count)
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

    // Download Singularity image if missing or outdated
    let sifCheck = try ssh(cluster: cluster, command: "test -f \(imageRemote) && echo exists || echo missing")
    let s3ImagePath = config.s3.singularityImagePath
    let s3HashPath: String
    if let dotIndex = s3ImagePath.lastIndex(of: ".") {
        s3HashPath = String(s3ImagePath[..<dotIndex]) + ".md5"
    } else {
        s3HashPath = s3ImagePath + ".md5"
    }

    var needsDownload = true
    if sifCheck.stdout.contains("exists") {
        // Compare local MD5 hash to remote .md5 file
        print("  Computing MD5 of local Singularity image...")
        let localHashResult = try ssh(cluster: cluster, command: "md5sum \(imageRemote) | awk '{print $1}'")
        let remoteHashResult = try ssh(cluster: cluster, command: "S3CMD_CONFIG=\(s3cfgRemote) s3cmd get \(s3HashPath) - 2>/dev/null | awk '{print $1}'")

        if localHashResult.succeeded && remoteHashResult.succeeded
            && !localHashResult.stdout.isEmpty && !remoteHashResult.stdout.isEmpty
            && localHashResult.stdout == remoteHashResult.stdout {
            print("  Singularity image is up to date (MD5 match).")
            needsDownload = false
        } else if remoteHashResult.succeeded && !remoteHashResult.stdout.isEmpty {
            print("  Singularity image is outdated (MD5 mismatch), re-downloading...")
        } else {
            print("  No remote .md5 found, skipping hash check. Image already present.")
            needsDownload = false
        }
    }

    if needsDownload {
        if sifCheck.stdout.contains("missing") {
            print("  Downloading Singularity image from S3 (this may take a while)...")
        }
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
    }
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
