from hate.configuration.gcloud_syncer import GCloudSync
obj = GCloudSync()

obj.sync_folder_from_gcloud("hatespeechnlp_proj", "dataset.zip", "dataset.zip")