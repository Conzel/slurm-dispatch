import Foundation

protocol Scheduler {
    var name: String { get }
    var jobIdEnvVar: String { get }

    func extractDirectives(from userScript: String) -> String
    func wrapperLogDirectives(repoDir: String) -> String
    func wrapperLogPaths(repoDir: String) -> (out: String, err: String)

    func extraSubmitFile(
        wrapperBaseName: String,
        userScript: String,
        repoDir: String
    ) -> (filename: String, content: String)?

    func submitTargetPath(wrapperBaseName: String) -> String
    func submitCommand(submitRelativePath: String) -> String
    func parseJobId(from stdout: String) throws -> String

    func postSubmitRenames(wrapperBaseName: String, jobId: String, repoDir: String) -> [String]

    func expectedStartTime(cluster: ClusterConfig, scriptPath: String) throws -> Date
}

// MARK: - SLURM

struct SlurmScheduler: Scheduler {
    let name = "slurm"
    let jobIdEnvVar = "SLURM_JOB_ID"

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

    func extraSubmitFile(wrapperBaseName: String, userScript: String, repoDir: String) -> (filename: String, content: String)? {
        return nil
    }

    func submitTargetPath(wrapperBaseName: String) -> String {
        return "logs/\(wrapperBaseName).sh"
    }

    func submitCommand(submitRelativePath: String) -> String {
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

    func extraSubmitFile(wrapperBaseName: String, userScript: String, repoDir: String) -> (filename: String, content: String)? {
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
        arguments               = "\(repoDir)/logs/\(wrapperBaseName).sh"
        output                  = \(repoDir)/logs/condor-$(Cluster).$(Process).out
        error                   = \(repoDir)/logs/condor-$(Cluster).$(Process).err
        log                     = \(repoDir)/logs/condor-$(Cluster).$(Process).log
        environment             = "JOB_ID=$(Cluster).$(Process)"
        getenv                  = false
        \(condorDirectives)
        queue 1
        """
        return ("\(wrapperBaseName).condor", content)
    }

    func submitTargetPath(wrapperBaseName: String) -> String {
        return "logs/\(wrapperBaseName).condor"
    }

    func submitCommand(submitRelativePath: String) -> String {
        return "condor_submit \(submitRelativePath)"
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
