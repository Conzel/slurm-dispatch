import Foundation

struct ShellResult {
    let stdout: String
    let stderr: String
    let exitCode: Int32

    var succeeded: Bool { exitCode == 0 }
    var combinedOutput: String {
        [stdout, stderr].filter { !$0.isEmpty }.joined(separator: "\n")
    }
}

func run(_ executable: String, _ args: [String]) throws -> ShellResult {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: executable)
    process.arguments = args

    let stdoutPipe = Pipe()
    let stderrPipe = Pipe()
    process.standardOutput = stdoutPipe
    process.standardError = stderrPipe

    try process.run()
    process.waitUntilExit()

    let stdout = String(data: stdoutPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?
        .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    let stderr = String(data: stderrPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?
        .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""

    return ShellResult(stdout: stdout, stderr: stderr, exitCode: process.terminationStatus)
}

func ssh(cluster: ClusterConfig, command: String, agentForward: Bool = false) throws -> ShellResult {
    var args = [
        "-i", cluster.expandedKeyPath,
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=10",
    ]
    if agentForward { args.append("-A") }
    args += ["\(cluster.user)@\(cluster.host)", command]
    return try run("/usr/bin/ssh", args)
}

func scp(localPath: String, cluster: ClusterConfig, remotePath: String) throws -> ShellResult {
    let args = [
        "-i", cluster.expandedKeyPath,
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=10",
        localPath,
        "\(cluster.user)@\(cluster.host):\(remotePath)"
    ]
    return try run("/usr/bin/scp", args)
}
