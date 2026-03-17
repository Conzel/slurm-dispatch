import Foundation

// MARK: - Argument parsing

let args = CommandLine.arguments.dropFirst()

if args.first == "--init" {
    guard !FileManager.default.fileExists(atPath: configPath) else {
        fputs("Config already exists at \(configPath). Remove it first if you want to reset.\n", stderr)
        exit(1)
    }
    do {
        try writeExampleConfig()
        print("Created example config at \(configPath)")
        print("Edit it with your cluster and S3 details, then run: submit <script.sh>")
        exit(0)
    } catch {
        fputs("Error creating config: \(error.localizedDescription)\n", stderr)
        exit(1)
    }
}

let scriptArgs = args.filter { !$0.hasPrefix("-") }

guard !scriptArgs.isEmpty else {
    fputs("Usage: submit <script.sh> [script2.sh ...]\n", stderr)
    fputs("       submit --init   (create example config at \(configPath))\n", stderr)
    exit(1)
}

let scriptPaths = scriptArgs.map { ($0 as NSString).expandingTildeInPath }

for scriptPath in scriptPaths {
    guard FileManager.default.fileExists(atPath: scriptPath) else {
        fputs("Error: script not found: \(scriptPath)\n", stderr)
        exit(1)
    }
}

// MARK: - Main flow

do {
    let config = try loadConfig()

    // Select the cluster with the earliest expected start time
    print("Checking expected start times...")
    let (clusterName, cluster) = try selectCluster(from: config.clusters, scriptPath: scriptPaths[0])
    print("Selected cluster: \(clusterName)")

    // Clone or update the repo first (creates remoteRepoDir)
    try setupRepo(cluster: cluster, config: config)

    // Validate cache (stage .s3cfg and .sif if needed — requires remoteRepoDir to exist)
    print("Validating remote cache on \(cluster.host)...")
    try validateCache(cluster: cluster, config: config)

    // Copy scripts and submit
    for scriptPath in scriptPaths {
        print("Dispatching job for \(URL(fileURLWithPath: scriptPath).lastPathComponent)...")
        let slurmJobId = try dispatchJob(
            cluster: cluster,
            config: config,
            scriptPath: scriptPath
        )
        print("Submitted batch job \(slurmJobId) on \(clusterName)")
    }

} catch {
    fputs("Error: \(error.localizedDescription)\n", stderr)
    exit(1)
}
