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
    let resultsFolderName = config.jobDefaults.localResultsFolderName
    let resultsBase = config.s3.resultsUploadPath.hasSuffix("/")
        ? config.s3.resultsUploadPath
        : config.s3.resultsUploadPath + "/"
    let logBase = config.s3.logsUploadPath.hasSuffix("/")
        ? config.s3.logsUploadPath
        : config.s3.logsUploadPath + "/"

    let lines = userScriptContent.components(separatedBy: .newlines)

    // Extract #SBATCH directives from user script to forward them to the wrapper
    let sbatchLines = lines
        .filter { $0.hasPrefix("#SBATCH") }
        .joined(separator: "\n")
    let sbatchSection = sbatchLines.isEmpty ? "" : sbatchLines + "\n"

    // Extract #REQUIRE S3 <path> directives from anywhere in the user script
    let s3RequirePaths = lines
        .compactMap { line -> String? in
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard trimmed.hasPrefix("#REQUIRE S3 ") else { return nil }
            let path = String(trimmed.dropFirst("#REQUIRE S3 ".count))
                .trimmingCharacters(in: .whitespaces)
            return path.isEmpty ? nil : path
        }

    // Generate download commands relative to repoDir (which is cd'd into during setup)
    let s3DownloadSection: String
    if s3RequirePaths.isEmpty {
        s3DownloadSection = ""
    } else {
        let downloadCommands = s3RequirePaths.map { path -> String in
            let destPath = "\(resultsFolderName)/\(path)"
            let destDir = (destPath as NSString).deletingLastPathComponent
            return "mkdir -p \(destDir) && s3cmd get \(resultsBase)\(path) \(destPath)"
        }.joined(separator: "\n")
        s3DownloadSection = "\n# == S3 Downloads (from #REQUIRE S3 directives) ==\n\(downloadCommands)\n"
    }

    return """
    #!/bin/bash
    \(sbatchSection)#SBATCH --output=\(repoDir)/logs/slurm-%j.out
    #SBATCH --error=\(repoDir)/logs/slurm-%j.err

    # == Setup Phase ==
    export S3CMD_CONFIG=\(s3cfgPath)
    [ -f \(repoDir)/.hf_token ] && export HF_TOKEN=$(cat \(repoDir)/.hf_token)
    cd \(repoDir)\(s3DownloadSection)

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

    # == Execution Phase ==
    # singularity exec \(imagePath) bash \(scriptName)
    bash \(scriptName)
    """
}
