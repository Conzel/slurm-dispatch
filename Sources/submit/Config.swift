import Foundation

struct Config: Decodable {
    let clusters: [String: ClusterConfig]
    let s3: S3Config
    let git: GitConfig
    let jobDefaults: JobDefaults

    enum CodingKeys: String, CodingKey {
        case clusters, s3, git
        case jobDefaults = "job_defaults"
    }
}

struct ClusterConfig: Decodable {
    let host: String
    let user: String
    let sshKeyPath: String
    let remoteRepoDir: String

    enum CodingKeys: String, CodingKey {
        case host, user
        case sshKeyPath = "ssh_key_path"
        case remoteRepoDir = "remote_repo_dir"
    }

    var expandedKeyPath: String {
        (sshKeyPath as NSString).expandingTildeInPath
    }
}

struct S3Config: Decodable {
    let localS3cfgFile: String
    let bucketUrl: String
    let singularityImagePath: String
    let resultsUploadPath: String
    let logsUploadPath: String

    enum CodingKeys: String, CodingKey {
        case localS3cfgFile = "local_s3cfg_file"
        case bucketUrl = "bucket_url"
        case singularityImagePath = "singularity_image_path"
        case resultsUploadPath = "results_upload_path"
        case logsUploadPath = "logs_upload_path"
    }

    var expandedS3cfgPath: String {
        (localS3cfgFile as NSString).expandingTildeInPath
    }

}

struct GitConfig: Decodable {
    let remoteUrl: String
    let remoteBranch: String

    enum CodingKeys: String, CodingKey {
        case remoteUrl = "remote_url"
        case remoteBranch = "remote_branch"
    }
}

struct JobDefaults: Decodable {
    let localResultsFolderName: String

    enum CodingKeys: String, CodingKey {
        case localResultsFolderName = "local_results_folder_name"
    }
}

let configDir = ("~/.config/submit" as NSString).expandingTildeInPath
let configPath = "\(configDir)/config.json"

let exampleConfig = """
{
  "clusters": {
    "cluster_a": {
      "host": "login.cluster-a.edu",
      "user": "jdoe",
      "ssh_key_path": "~/.ssh/id_rsa_cluster_a",
      "remote_repo_dir": "/scratch/jdoe/repo"
    },
    "cluster_b": {
      "host": "login.cluster-b.com",
      "user": "jdoe",
      "ssh_key_path": "~/.ssh/id_rsa_cluster_b",
      "remote_repo_dir": "/work/group/jdoe/repo"
    }
  },
  "git": {
    "remote_url": "git@github.com:your-org/your-repo.git",
    "remote_branch": "main"
  },
  "s3": {
    "local_s3cfg_file": "~/.my_s3_configs/.s3cfg",
    "bucket_url": "s3://my-experiment-bucket",
    "singularity_image_path": "s3://my-experiment-bucket/images/container.sif",
    "results_upload_path": "s3://my-experiment-bucket/results/",
    "logs_upload_path": "s3://my-experiment-bucket/logs/"
  },
  "job_defaults": {
    "local_results_folder_name": "results"
  }
}
"""

func loadConfig() throws -> Config {
    let url = URL(fileURLWithPath: configPath)
    guard FileManager.default.fileExists(atPath: configPath) else {
        throw ConfigError.notFound
    }
    let data = try Data(contentsOf: url)
    return try JSONDecoder().decode(Config.self, from: data)
}

func writeExampleConfig() throws {
    try FileManager.default.createDirectory(atPath: configDir, withIntermediateDirectories: true)
    try exampleConfig.write(toFile: configPath, atomically: true, encoding: .utf8)
}

enum ConfigError: Error, LocalizedError {
    case notFound

    var errorDescription: String? {
        """
        No config found at \(configPath).
        Run `submit --init` to create an example config, then edit it with your cluster details.
        """
    }
}
