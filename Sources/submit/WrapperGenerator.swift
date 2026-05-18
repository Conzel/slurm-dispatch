import Foundation

func generateWrapper(
    scheduler: Scheduler,
    cluster: ClusterConfig,
    config: Config,
    repoDir: String,
    scriptName: String,
    userScriptContent: String
) -> String {
    let imagePath = "\(repoDir)/container.sif"
    let s3cfgPath = "\(repoDir)/.s3cfg"
    let resultsFolderName = "results"
    let bucketUrl = config.s3.bucketUrl.hasSuffix("/") ? String(config.s3.bucketUrl.dropLast()) : config.s3.bucketUrl
    let resultsBase = "\(bucketUrl)/results/"
    let logBase = "\(bucketUrl)/logs/"

    // Scheduler-specific directives forwarded into the wrapper preamble.
    let directiveSection = scheduler.extractDirectives(from: userScriptContent)
    let logDirectives = scheduler.wrapperLogDirectives(repoDir: repoDir)
    let logDirectivesSection = logDirectives.isEmpty ? "" : logDirectives + "\n"

    let jobIdVar = scheduler.jobIdEnvVar
    let logPaths = scheduler.wrapperLogPaths(repoDir: repoDir)
    let jobIdInit = scheduler.wrapperJobIdInit()
    let jobIdInitSection = jobIdInit.isEmpty ? "" : jobIdInit + "\n    "

    // Extract #REQUIRE S3 [OVERWRITE|SKIP] <path> directives (scheduler-agnostic).
    struct S3Require {
        let path: String
        let overwrite: Bool
    }
    let s3Requires: [S3Require] = userScriptContent
        .components(separatedBy: .newlines)
        .compactMap { line -> S3Require? in
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
            return rest.isEmpty ? nil : S3Require(path: rest, overwrite: overwrite)
        }

    let s3RequireSection: String
    if s3Requires.isEmpty || !scheduler.executeHasS3Egress {
        // Either no S3 requirements, or execute nodes can't reach S3 (in which
        // case the dispatcher pre-staged them on the login node — see
        // Dispatcher.preStageS3Requirements).
        s3RequireSection = ""
    } else {
        let commands = s3Requires.map { req -> String in
            let remotePath = "\(resultsBase)\(req.path)"
            let localPath = "\(repoDir)/\(resultsFolderName)/\(req.path)"
            let localDir = (localPath as NSString).deletingLastPathComponent
            let s3Flag = req.overwrite ? "--force" : "--skip-existing"
            return """
                mkdir -p \(localDir)
                echo "[submit] Downloading \(remotePath) -> \(localPath)"
                s3cmd get \(s3Flag) \(remotePath) \(localPath)
            """
        }.joined(separator: "\n")
        s3RequireSection = """

        # == S3 Requirements ==
        \(commands)

        """
    }

    return """
    #!/bin/bash
    \(directiveSection)\(logDirectivesSection)
    # == Setup Phase ==
    \(jobIdInitSection)# HTCondor often points HOME at the scratch dir, so resolve the real one
    # from passwd. Harmless under SLURM (overwrites HOME with the same value).
    export HOME="$(getent passwd "$(id -u)" | cut -d: -f6)"
    export PATH="$HOME/.local/bin:$PATH"
    export S3CMD_CONFIG=\(s3cfgPath)
    [ -f \(repoDir)/.hf_token ] && export HF_TOKEN=$(cat \(repoDir)/.hf_token)
    cd \(repoDir)

    \(scheduler.executeHasS3Egress ? cleanupTrapSection(
        repoDir: repoDir,
        resultsFolderName: resultsFolderName,
        resultsBase: resultsBase,
        logPaths: logPaths,
        logBase: logBase,
        jobIdVar: jobIdVar
    ) : "# (no in-job S3 teardown — execute nodes lack egress; upload runs on the login node post-completion)")

    \(s3RequireSection)# == Execution Phase ==
    # singularity exec \(imagePath) bash \(scriptName)
    bash \(scriptName)
    """
}

private func cleanupTrapSection(
    repoDir: String,
    resultsFolderName: String,
    resultsBase: String,
    logPaths: (out: String, err: String),
    logBase: String,
    jobIdVar: String
) -> String {
    return """
    # == Teardown Trap ==
    cleanup() {
        cd \(repoDir)
        if [ -d "\(resultsFolderName)" ]; then
            s3cmd sync "\(resultsFolderName)/" "\(resultsBase)\(resultsFolderName)/" || true
        else
            echo "[submit] No results found at \(resultsFolderName)"
        fi

        if [ -f "\(logPaths.out)" ]; then
            s3cmd put "\(logPaths.out)" "\(logBase)${\(jobIdVar)}/" || true
        else
            echo "[submit] No log found at \(logPaths.out)"
        fi

        if [ -f "\(logPaths.err)" ]; then
            s3cmd put "\(logPaths.err)" "\(logBase)${\(jobIdVar)}/" || true
        else
            echo "[submit] No log found at \(logPaths.err)"
        fi
    }
    trap cleanup EXIT ERR TERM
    """
}
