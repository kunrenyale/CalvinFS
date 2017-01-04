// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#ifndef CALVIN_FS_LOCAL_CALVINFS_H_
#define CALVIN_FS_LOCAL_CALVINFS_H_

#include "components/fs/fs.h"
#include "common/types.h"

class LocalFileHandle : public FileHandle {
 public:
  virtual ~LocalFileHandle() {}

  virtual uint64 Version() {
    return version_;
  }

  virtual uint64 Size() {
    uint64 size = 0;
    for (int i = 0; i < metadata_.data_blocks_size(); i++) {
      size += metadata_.data_blocks(i).block_size();
    }
    return size;
  }

  virtual Status ReadToString(uint64 offset, uint64 count, string* result) {
    CHECK(offset + count <= Size());
    result->clear();
    if (count == 0) {
      return Status::OK(version_);
    }
    uint64 current_block = 0;
    uint64 current_offset = 0;
    // Find first block that overlaps read region.
    while (current_block < metadata_.data_blocks_size() &&
           current_offset + metadata_.data_blocks(current_block).block_size()
            < offset) {
      current_offset += metadata_.data_blocks(current_block).block_size();
      current_block++;
    }

    // Read all blocks fully contained in read region.
    while (current_block < metadata_.data_blocks_size() &&
           current_offset < offset + count) {
      // TODO(agt): Reduce copying here!
      string block_contents;
      if (!fs_->blocks_->Get(
          metadata_.data_blocks(current_block).block_id(),
          &block_contents)) {
        return Status::Error("failure to read block", version_);
      }
      result->append(block_contents);
      current_offset += metadata_.data_blocks(current_block).block_size();
      current_block++;
    }

    if (result->size() < count) {
      // We failed to read the entire amount requested for some reason.
      return Status::Error("too few bytes read", version_);
    } else if (result->size() > count) {
      // Truncate result if the final block went past the end of the region we
      // were trying to read.
      result->resize(count);
    }
    return Status::OK("failure to read block", version_);
  }

  virtual Status GetPermissions(uint64* owner, uint64* group, string* mode) {
    *owner = metadata_.owner();
    *group = metadata_.group();
    *mode = metadata_.mode();
    return Status::OK(version_);
  }

 private:
  friend class LocalFileSystem;

  // File path.
  string path_;

  // Version at which metadata was read.
  uint64 version_;

  // File metadata.
  MetadataEntry metadata_;

  // Pointer to FS in which file lives.
  LocalFileSystem* fs_;
};

string GetParentPath(const string& path) {
  // TODO
}

class LocalFileSystem : public FileSystem{
 public:
  explicit LocalFileSystem(const string& path_prefix)
      : version_(0), next_block_id_(0) {
    metadata_ = new MVMapStore();
    blocks_ = new LocalFileBlockStore(path_prefix);
  }

  virtual Status Open(const string& path, FileHandle** file) {
    uint64 version = version_.load();

    // Get metadata.
    string file_metadata;
    if (!metadata_->Get(path, version, &file_metadata)) {
      return Status::Error("failure to read metadata", version);
    }

    // Create file handle.
    *file = new LocalFileHandle();
    (*file)->path_ = path;
    (*file)->version_ = version;
    (*file)->metadata_.ParseFromString(file_metadata);
    (*file)->fs_ = this;

    return Status::OK(version);
  }

  virtual Status ReadFileToString(const string& path, string* data) {
    FileHandle* file;
    Status s = Open(path, &file);
    if (s.ok()) {
      s = file->ReadToString(0, file->Size(), data);
      delete file;
    }
    return s;
  }

  virtual Status CreateDirectory(const string& path) {
    // Write metadata.
    uint64 version = version_++;
    if (metadata_->Exists(GetParentPath(path), version) &&
        !metadata_->Exists(path, version)) {
      FileMetadata md;
      md.set_is_dir(true);
      string md_str;
      md.SerializeToString(md_str);
      metadata_->Put(path, md_str, version);
      return Status::OK(version);
    } else {
      return Status::Error("parent directory does not exist", version);
    }
  }

  virtual Status CreateFile(const string& path) {
    // Write metadata.
    uint64 version = version_++;
    if (metadata_->Exists(GetParentPath(path), version) &&
        !metadata_->Exists(path, version)) {
      FileMetadata md;
      string md_str;
      md.SerializeToString(md_str);
      metadata_->Put(path, md_str, version);
      return Status::OK(version);
    } else {
      return Status::Error("parent directory does not exist", version);
    }
  }

  virtual Status WriteStringToFile(const string& data, const string& path) {
    // Write block.
    uint64 block_id = next_block_id_++;
    blocks_->Put(block_id, data);

    // Write metadata.
    uint64 version = version_++;
    if (metadata_->Exists(GetParentPath(path), version) &&
        !metadata_->Exists(path, version)) {
      FileMetadata md;
      md.add_data_blocks();
      md.data_blocks(0).set_block_id(block_id);
      md.data_blocks(0).set_block_size(path.size());
      string md_str;
      md.SerializeToString(md_str);
      metadata_->Put(path, md_str, version);
      return Status::OK(version);
    } else {
      return Status::Error("parent directory does not exist", version);
    }
  }

  virtual Status AppendStringToFile(const string& data, const string& path) {
    return Status::Error("not implemented");
  }

  virtual Status AppendFileToFile(const string& data, const string& path) {
    return Status::Error("not implemented");
  }

  virtual Status SetPermissions(
      uint64 owner,
      uint64 group,
      const string& mode,
      const string& path) {
    return Status::Error("not implemented");
  }

  virtual Status GetPermissions(
      const string& path,
      uint64* owner,
      uint64* group,
      string* mode) {
    FileHandle* file;
    Status s = Open(path, &file);
    if (s.ok()) {
      s = file->GetPermissions(owner, group, mode);
      delete file;
    }
    return s;
  }

  virtual Status Remove(const string& path) {
    ///
  }

  virtual Status Copy(const string& from_path, const string& to_path) {
    ///
  }

  virtual Status Rename(const string& from_path, const string& to_path) {
    return Status::Error("not implemented");
  }

  virtual Status LS(const string& path, vector<string>* contents) {
    ///
  }

 private:
  atomic<uint64> next_block_id_;
  atomic<uint64> version_;
  MVStore* metadata_;
  BlockStore* blocks_;
};

#endif  // CALVIN_FS_LOCAL_CALVINFS_H_

