import Foundation

protocol Scheduler {
    var name: String { get }
    var jobIdEnvVar: String { get }
    /// `true` if execute nodes have outbound network to S3 (so the wrapper can
    /// run s3cmd at job runtime). `false` means the dispatcher must pre-stage
    /// `#REQUIRE S3` artifacts via SSH on the login node and the wrapper must
    /// not touch S3 at all.
    var executeHasS3Egress: Bool { get }

    func extractDirectives(from userScript: String) -> String
    func wrapperLogDirectives(repoDir: String) -> String
    func wrapperLogPaths(repoDir: String) -> (out: String, err: String)
    /// Shell snippet inserted near the top of the wrapper that defines the
    /// `$JOB_ID` env var (used by cleanup + log upload). Returns "" if the
    /// scheduler already exports it (e.g. SLURM via $SLURM_JOB_ID).
    func wrapperJobIdInit() -> String

    func extraSubmitFile(
        wrapperBaseName: String,
        userScript: String,
        repoDir: String
    ) -> (filename: String, content: String)?

    func submitTargetPath(wrapperBaseName: String) -> String
    func submitCommand(submitRelativePath: String, userScript: String) -> String
    func parseJobId(from stdout: String) throws -> String

    func postSubmitRenames(wrapperBaseName: String, jobId: String, repoDir: String) -> [String]

    /// An SSH command to run on the login node right after submit, detached, to
    /// handle post-completion work (e.g. uploading results to S3 once the job
    /// finishes — needed when execute nodes have no S3 egress). Returns nil if
    /// the scheduler handles uploads in-job.
    func postDispatchCommand(jobId: String, repoDir: String, config: Config) -> String?

    func expectedStartTime(cluster: ClusterConfig, scriptPath: String) throws -> Date
}

// MARK: - SLURM

struct SlurmScheduler: Scheduler {
    let name = "slurm"
    let jobIdEnvVar = "SLURM_JOB_ID"
    let executeHasS3Egress = true

    func extractDirectives(from userScript: String) -> String {
        let lines = userScript
            .components(separatedBy: .newlines)
            .filter { $0.hasPrefix("#SBATCH") }
            .joined(separator: "\n")
        return lines.isEmpty ? "" : lines + "\n"
    }

    func wrapperLogDirectives(repoDir: String) -> String {
        return """
        #SBATCH --output=\(repoDir)/logs/slurm-%j.out
        #SBATCH --error=\(repoDir)/logs/slurm-%j.err
        """
    }

    func wrapperLogPaths(repoDir: String) -> (out: String, err: String) {
        return (
            "logs/slurm-${SLURM_JOB_ID}.out",
            "logs/slurm-${SLURM_JOB_ID}.err"
        )
    }

    func wrapperJobIdInit() -> String { return "" }

    func extraSubmitFile(
        wrapperBaseName: String,
        userScript: String,
        repoDir: String
    ) -> (filename: String, content: String)? {
        return nil
    }

    func submitTargetPath(wrapperBaseName: String) -> String {
        return "logs/\(wrapperBaseName).sh"
    }

    func submitCommand(submitRelativePath: String, userScript: String) -> String {
        return "sbatch \(submitRelativePath)"
    }

    func parseJobId(from stdout: String) throws -> String {
        guard let id = stdout.components(separatedBy: .whitespaces).last, !id.isEmpty else {
            throw DispatchError.sbatchOutputUnexpected(stdout)
        }
        return id
    }

    func postSubmitRenames(wrapperBaseName: String, jobId: String, repoDir: String) -> [String] {
        return ["mv \(repoDir)/logs/\(wrapperBaseName).sh \(repoDir)/logs/wrapper_\(jobId).sh"]
    }

    func postDispatchCommand(jobId: String, repoDir: String, config: Config) -> String? {
        // SLURM execute nodes have S3 egress; the wrapper uploads in-job.
        return nil
    }

    func expectedStartTime(cluster: ClusterConfig, scriptPath: String) throws -> Date {
        let parentDir = (cluster.remoteRepoDir as NSString).deletingLastPathComponent
        let remoteTmp = "\(parentDir)/.submit_test_\(UUID().uuidString).sbatch"
        defer { _ = try? ssh(cluster: cluster, command: "rm -f \(remoteTmp)") }

        let scpResult = try scp(localPath: scriptPath, cluster: cluster, remotePath: remoteTmp)
        guard scpResult.succeeded else {
            throw DispatchError.scpFailed(file: scriptPath, output: scpResult.combinedOutput)
        }

        let result = try ssh(cluster: cluster, command: "sbatch --test-only \(remoteTmp) 2>&1")
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
}

// MARK: - HTCondor

struct CondorScheduler: Scheduler {
    let name = "htcondor"
    let jobIdEnvVar = "JOB_ID"
    let executeHasS3Egress = false  // MPI-IS execute nodes have no outbound network

    func extractDirectives(from userScript: String) -> String {
        return ""
    }

    func wrapperLogDirectives(repoDir: String) -> String {
        return ""
    }

    func wrapperLogPaths(repoDir: String) -> (out: String, err: String) {
        return (
            "logs/condor-${JOB_ID}.out",
            "logs/condor-${JOB_ID}.err"
        )
    }

    func wrapperJobIdInit() -> String {
        // The .condor file passes "$(Cluster).$(Process)" as the first positional
        // arg to /bin/bash (see arguments line in extraSubmitFile). HTCondor
        // expands those macros at submit time. `environment="JOB_ID=..."` is
        // unreliable across condor versions, so we go through $1 instead.
        // Exported so the user script (run as a child `bash`) inherits it.
        return "export JOB_ID=\"$1\""
    }

    func extraSubmitFile(
        wrapperBaseName: String,
        userScript: String,
        repoDir: String
    ) -> (filename: String, content: String)? {
        let condorDirectives = userScript
            .components(separatedBy: .newlines)
            .compactMap { line -> String? in
                let trimmed = line.trimmingCharacters(in: .whitespaces)
                guard trimmed.hasPrefix("#CONDOR ") else { return nil }
                return String(trimmed.dropFirst("#CONDOR ".count))
            }
            .joined(separator: "\n")

        let content = """
        universe                = vanilla
        executable              = /bin/bash
        arguments               = "\(repoDir)/logs/\(wrapperBaseName).sh $(Cluster).$(Process)"
        output                  = \(repoDir)/logs/condor-$(Cluster).$(Process).out
        error                   = \(repoDir)/logs/condor-$(Cluster).$(Process).err
        log                     = \(repoDir)/logs/condor-$(Cluster).$(Process).log
        getenv                  = false
        \(condorDirectives)
        queue 1
        """
        return ("\(wrapperBaseName).condor", content)
    }

    func submitTargetPath(wrapperBaseName: String) -> String {
        return "logs/\(wrapperBaseName).condor"
    }

    func submitCommand(submitRelativePath: String, userScript: String) -> String {
        // `#CONDOR_BID <N>` in the user script switches us onto MPI-IS's
        // condor_submit_bid wrapper, which takes the bid as a CLI arg
        // (not as a submit-file directive).
        if let bid = parseBid(from: userScript) {
            return "condor_submit_bid \(bid) \(submitRelativePath)"
        }
        return "condor_submit \(submitRelativePath)"
    }

    private func parseBid(from userScript: String) -> Int? {
        for line in userScript.components(separatedBy: .newlines) {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard trimmed.hasPrefix("#CONDOR_BID ") else { continue }
            let rest = String(trimmed.dropFirst("#CONDOR_BID ".count))
                .trimmingCharacters(in: .whitespaces)
            return Int(rest)
        }
        return nil
    }

    func parseJobId(from stdout: String) throws -> String {
        // condor_submit prints: "1 job(s) submitted to cluster 12345."
        guard let range = stdout.range(of: "submitted to cluster ") else {
            throw DispatchError.sbatchOutputUnexpected(stdout)
        }
        let rest = stdout[range.upperBound...]
        let id = String(rest.prefix(while: { $0.isNumber }))
        guard !id.isEmpty else {
            throw DispatchError.sbatchOutputUnexpected(stdout)
        }
        return id
    }

    func postSubmitRenames(wrapperBaseName: String, jobId: String, repoDir: String) -> [String] {
        // Keep the .sh wrapper at its original path — the queued condor job's
        // `arguments` line points at it. Only rename the .condor for human bookkeeping.
        return ["mv \(repoDir)/logs/\(wrapperBaseName).condor \(repoDir)/logs/wrapper_\(jobId).condor"]
    }

    func postDispatchCommand(jobId: String, repoDir: String, config: Config) -> String? {
        // Compute nodes have no S3 egress, so the upload runs on the login node.
        // Launch a detached watcher: condor_wait blocks until the job's event log
        // shows termination, then s3cmd sync runs (login node has egress).
        // The watcher outlives the SSH session via nohup + full FD redirection.
        let bucketUrl = config.s3.bucketUrl.hasSuffix("/")
            ? String(config.s3.bucketUrl.dropLast()) : config.s3.bucketUrl
        let resultsBase = "\(bucketUrl)/results/"
        let logBase = "\(bucketUrl)/logs/"
        let s3cfgPath = "\(repoDir)/.s3cfg"
        let jobLog = "\(repoDir)/logs/condor-\(jobId).0.log"
        let watcherLog = "\(repoDir)/logs/sync_\(jobId).log"

        // Single-quoted bash -c body — contains no single quotes.
        let body = """
        export HOME="$(getent passwd "$(id -u)" | cut -d: -f6)"
        export PATH="$HOME/.local/bin:$PATH"
        export S3CMD_CONFIG=\(s3cfgPath)
        echo "[submit/watch] waiting for job \(jobId) to finish..."
        condor_wait \(jobLog)
        cd \(repoDir)
        echo "[submit/watch] job finished; uploading results"
        s3cmd sync results/ \(resultsBase) || true
        for stream in out err; do
          f=logs/condor-\(jobId).0.$stream
          [ -f "$f" ] && s3cmd put "$f" \(logBase)\(jobId)/ || true
        done
        echo "[submit/watch] done"
        """

        return "nohup bash -c '\(body)' > \(watcherLog) 2>&1 < /dev/null &"
    }

    func expectedStartTime(cluster: ClusterConfig, scriptPath: String) throws -> Date {
        // No condor_submit equivalent of `sbatch --test-only`. Use queue depth as a proxy:
        // count the user's idle+running jobs and assume one-minute-per-job latency.
        let result = try ssh(
            cluster: cluster,
            command: "condor_q $USER -nobatch -af ClusterId 2>/dev/null | wc -l"
        )
        let count = Int(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 0
        return Date(timeIntervalSinceNow: TimeInterval(count * 60))
    }
}

// MARK: - Factory

/// Map a script's file extension to the scheduler that should run it.
/// Returns nil for unknown extensions — callers should refuse to dispatch.
func schedulerName(forScriptPath path: String) -> String? {
    let ext = (path as NSString).pathExtension.lowercased()
    switch ext {
    case "sbatch": return "slurm"
    case "htcondor": return "htcondor"
    default: return nil
    }
}

func makeScheduler(_ name: String?) -> Scheduler {
    switch (name ?? "slurm").lowercased() {
    case "htcondor", "condor":
        return CondorScheduler()
    case "slurm", "":
        return SlurmScheduler()
    default:
        fputs("Warning: unknown scheduler '\(name ?? "")', defaulting to slurm.\n", stderr)
        return SlurmScheduler()
    }
}
