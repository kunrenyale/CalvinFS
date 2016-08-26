# Create template specific variables
UC_DIR := FS
LC_DIR := fs

SRCS := fs/metadata_store.cc \
        fs/block_store.cc \
        fs/block_log.cc \
        fs/localfs.cc \
        fs/hdfs.cc \
        fs/calvinfs.cc \
        fs/calvinfs_client_app.cc
EXES :=
TEST := fs/block_store_test.cc \
        fs/block_log_test.cc \
        fs/metadata_store_test.cc \
        fs/fs_test.cc
PROTOS := fs/metadata.proto \
          fs/calvinfs_config.proto \
          fs/batch.proto

DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(MACHINE_OBJS) $(COMPONENTS_OBJS)

################################################################################

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-fs: $(FS_TESTS)
	@for a in $(FS_TESTS); do \
    echo "";\
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
