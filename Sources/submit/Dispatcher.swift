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
            command: "PATH=\"$HOME/.local/bin:$PATH\" S3CMD_CONFIG=\(s3cfgRemote) s3cmd get --force \(s3ImagePath) \(imageRemote)"
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
            command: "PATH=\"$HOME/.local/bin:$PATH\" S3CMD_CONFIG=\(s3cfgRemote) s3cmd get --force \(s3HashPath) \(md5Tmp)"
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

func selectCluster(from clusters: [String: ClusterConfig], scriptPath: String) throws -> (name: String, config: ClusterConfig) {
    let timeFormatter = DateFormatter()
    timeFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss"

    var best: (name: String, config: ClusterConfig, startTime: Date)?

    for (name, cluster) in clusters {
        do {
            let startTime = try cluster.schedulerImpl.expectedStartTime(cluster: cluster, scriptPath: scriptPath)
            print("  \(name) (\(cluster.host), \(cluster.schedulerImpl.name)): expected start at \(timeFormatter.string(from: startTime))")
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
    let scheduler = cluster.schedulerImpl
    let repoDir = cluster.remoteRepoDir
    let scriptName = URL(fileURLWithPath: scriptPath).lastPathComponent
    let userScriptContent = try String(contentsOfFile: scriptPath, encoding: .utf8)

    // Generate wrapper script.
    let wrapperContent = generateWrapper(
        scheduler: scheduler,
        cluster: cluster,
        config: config,
        repoDir: repoDir,
        scriptName: scriptName,
        userScriptContent: userScriptContent
    )

    // Local temp file for the wrapper.
    let wrapperBaseName = "wrapper_tmp_\(UUID().uuidString)"
    let tmpWrapper = FileManager.default.temporaryDirectory
        .appendingPathComponent("\(wrapperBaseName).sh")
    try wrapperContent.write(to: tmpWrapper, atomically: true, encoding: .utf8)
    defer { try? FileManager.default.removeItem(at: tmpWrapper) }

    // SCP user script into repo root.
    let scpScript = try scp(localPath: scriptPath, cluster: cluster, remotePath: "\(repoDir)/\(scriptName)")
    guard scpScript.succeeded else {
        throw DispatchError.scpFailed(file: scriptName, output: scpScript.combinedOutput)
    }

    // Ensure logs directory exists before SCP-ing into it.
    let mklogsResult = try ssh(cluster: cluster, command: "mkdir -p \(repoDir)/logs")
    guard mklogsResult.succeeded else {
        throw DispatchError.sshFailed(host: cluster.host, command: "mkdir -p \(repoDir)/logs", output: mklogsResult.combinedOutput)
    }

    // SCP wrapper into logs/.
    let wrapperRemote = "\(repoDir)/logs/\(wrapperBaseName).sh"
    let scpWrapper = try scp(localPath: tmpWrapper.path, cluster: cluster, remotePath: wrapperRemote)
    guard scpWrapper.succeeded else {
        throw DispatchError.scpFailed(file: "\(wrapperBaseName).sh", output: scpWrapper.combinedOutput)
    }

    // If execute nodes can't reach S3, pre-stage #REQUIRE S3 artifacts via SSH
    // on the login node now, before the job runs.
    if !scheduler.executeHasS3Egress {
        try preStageS3Requirements(
            cluster: cluster,
            config: config,
            repoDir: repoDir,
            userScriptContent: userScriptContent
        )
    }

    // Generate + upload a scheduler-specific submit-file companion (e.g. .condor), if any.
    if let extra = scheduler.extraSubmitFile(
        wrapperBaseName: wrapperBaseName,
        userScript: userScriptContent,
        repoDir: repoDir
    ) {
        let tmpExtra = FileManager.default.temporaryDirectory.appendingPathComponent(extra.filename)
        try extra.content.write(to: tmpExtra, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(at: tmpExtra) }
        let extraRemote = "\(repoDir)/logs/\(extra.filename)"
        let scpExtra = try scp(localPath: tmpExtra.path, cluster: cluster, remotePath: extraRemote)
        guard scpExtra.succeeded else {
            throw DispatchError.scpFailed(file: extra.filename, output: scpExtra.combinedOutput)
        }
    }

    // Submit.
    let submitTarget = scheduler.submitTargetPath(wrapperBaseName: wrapperBaseName)
    let submitCmd = scheduler.submitCommand(submitRelativePath: submitTarget, userScript: userScriptContent)
    let submitResult = try ssh(
        cluster: cluster,
        command: "cd \(repoDir) && \(submitCmd)"
    )
    guard submitResult.succeeded else {
        throw DispatchError.sshFailed(host: cluster.host, command: submitCmd, output: submitResult.combinedOutput)
    }

    let jobId = try scheduler.parseJobId(from: submitResult.stdout)

    // Post-submit housekeeping (rename temp wrapper artifacts to include the job id).
    for cmd in scheduler.postSubmitRenames(wrapperBaseName: wrapperBaseName, jobId: jobId, repoDir: repoDir) {
        _ = try ssh(cluster: cluster, command: cmd)
    }

    // Launch a detached post-completion watcher on the login node if the
    // scheduler needs one (HTCondor: uploads results to S3 after the job ends,
    // since compute nodes have no egress).
    if let watchCmd = scheduler.postDispatchCommand(jobId: jobId, repoDir: repoDir, config: config) {
        let result = try ssh(cluster: cluster, command: watchCmd)
        if result.succeeded {
            print("  Started login-node upload watcher (logs/sync_\(jobId).log)")
        } else {
            fputs("  WARNING: failed to start upload watcher: \(result.combinedOutput)\n", stderr)
        }
    }

    return jobId
}

// MARK: - S3 pre-staging (for execute nodes without S3 egress)

func preStageS3Requirements(
    cluster: ClusterConfig,
    config: Config,
    repoDir: String,
    userScriptContent: String
) throws {
    struct S3Req { let path: String; let overwrite: Bool }
    let reqs: [S3Req] = userScriptContent
        .components(separatedBy: .newlines)
        .compactMap { line in
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard trimmed.hasPrefix("#REQUIRE S3 ") else { return nil }
            var rest = String(trimmed.dropFirst("#REQUIRE S3 ".count))
                .trimmingCharacters(in: .whitespaces)
            guard !rest.isEmpty else { return nil }
            var overwrite = false
            if rest.hasPrefix("OVERWRITE ") {
                overwrite = true
                rest = String(rest.dropFirst("OVERWRITE ".count)).trimmingCharacters(in: .whitespaces)
            } else if rest.hasPrefix("SKIP ") {
                rest = String(rest.dropFirst("SKIP ".count)).trimmingCharacters(in: .whitespaces)
            }
            return rest.isEmpty ? nil : S3Req(path: rest, overwrite: overwrite)
        }
    guard !reqs.isEmpty else { return }

    let bucketUrl = config.s3.bucketUrl.hasSuffix("/")
        ? String(config.s3.bucketUrl.dropLast()) : config.s3.bucketUrl
    let resultsBase = "\(bucketUrl)/results/"
    let s3cfgRemote = "\(repoDir)/.s3cfg"

    print("Pre-staging \(reqs.count) S3 artifact(s) on \(cluster.host)...")
    for req in reqs {
        // A trailing slash means "directory": recursive get of an S3 prefix.
        let isDir = req.path.hasSuffix("/")
        let remotePath = "\(resultsBase)\(req.path)"
        let localPath = "\(repoDir)/results/\(req.path)"
        let flag = req.overwrite ? "--force" : "--skip-existing"
        let cmd: String
        if isDir {
            cmd = """
            mkdir -p \(localPath) && \
            PATH="$HOME/.local/bin:$PATH" S3CMD_CONFIG=\(s3cfgRemote) \
            s3cmd get --recursive \(flag) \(remotePath) \(localPath)
            """
        } else {
            let localDir = (localPath as NSString).deletingLastPathComponent
            cmd = """
            mkdir -p \(localDir) && \
            PATH="$HOME/.local/bin:$PATH" S3CMD_CONFIG=\(s3cfgRemote) \
            s3cmd get \(flag) \(remotePath) \(localPath)
            """
        }
        print("  \(req.path) (\(req.overwrite ? "force" : "skip-existing")\(isDir ? ", recursive" : ""))")
        let result = try ssh(cluster: cluster, command: cmd)
        guard result.succeeded else {
            throw DispatchError.sshFailed(
                host: cluster.host,
                command: "s3cmd get \(req.path)",
                output: result.combinedOutput
            )
        }
    }
}
