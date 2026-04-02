import Foundation

func generateWrapper(
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

    // Extract #SBATCH directives from user script to forward them to the wrapper
    let sbatchLines = userScriptContent
        .components(separatedBy: .newlines)
        .filter { $0.hasPrefix("#SBATCH") }
        .joined(separator: "\n")
    let sbatchSection = sbatchLines.isEmpty ? "" : sbatchLines + "\n"

    // Extract #REQUIRE S3 [OVERWRITE|SKIP] <path> directives
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
    if s3Requires.isEmpty {
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
    \(sbatchSection)#SBATCH --output=\(repoDir)/logs/slurm-%j.out
    #SBATCH --error=\(repoDir)/logs/slurm-%j.err

    # == Setup Phase ==
    export S3CMD_CONFIG=\(s3cfgPath)
    [ -f \(repoDir)/.hf_token ] && export HF_TOKEN=$(cat \(repoDir)/.hf_token)
    cd \(repoDir)

    # == Teardown Trap ==
    cleanup() {
        cd \(repoDir)
        if [ -d "\(resultsFolderName)" ]; then
            s3cmd sync "\(resultsFolderName)/" "\(resultsBase)\(resultsFolderName)/" || true
        else
            echo "[submit] No results found at \(resultsFolderName)"
        fi

        if [ -f "logs/slurm-${SLURM_JOB_ID}.out" ]; then
            s3cmd put "logs/slurm-${SLURM_JOB_ID}.out" "\(logBase)${SLURM_JOB_ID}/" || true
        else
            echo "[submit] No log found at logs/slurm-${SLURM_JOB_ID}.out"
        fi

        if [ -f "logs/slurm-${SLURM_JOB_ID}.err" ]; then
            s3cmd put "logs/slurm-${SLURM_JOB_ID}.err" "\(logBase)${SLURM_JOB_ID}/" || true
        else
            echo "[submit] No log found at logs/slurm-${SLURM_JOB_ID}.err"
        fi
    }
    trap cleanup EXIT ERR TERM

    \(s3RequireSection)# == Execution Phase ==
    # singularity exec \(imagePath) bash \(scriptName)
    bash \(scriptName)
    """
}
