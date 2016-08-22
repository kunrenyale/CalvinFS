# Create template specific variables
UC_DIR := SCRIPTS
LC_DIR := scripts

SRCS := scripts/script_utils.cc

EXES := scripts/cfs.cc \
        scripts/cluster.cc \
        scripts/calvin_server.cc \
        scripts/calvinfs_server.cc

DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(MACHINE_OBJS) $(COMPONENTS_OBJS) $(FS_OBJS)

################################################################################

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-scripts: $(SCRIPTS_TESTS)
	@for a in $(SCRIPTS_TESTS); do \
    echo "";\
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done

