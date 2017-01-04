# Create template specific variables
UC_DIR := COMPONENTS
LC_DIR := components

SRCS := components/log/local_mem_log.cc \
        components/log/paxos.cc \
        components/log/paxos2.cc \
        components/log/log_app.cc \
        components/store/store_app.cc \
        components/store/kvstore.cc \
        components/store/btreestore.cc \
        components/store/leveldbstore.cc \
        components/store/versioned_kvstore.cc \
        components/store/hybrid_versioned_kvstore.cc \
        components/scheduler/lock_manager.cc \
        components/scheduler/serial_scheduler.cc \
        components/scheduler/locking_scheduler.cc \
        components/scheduler/vll_scheduler.cc

EXES :=

TEST := components/log/local_mem_log_test.cc \
        components/log/paxos_test.cc \
        components/log/paxos2_test.cc \
        components/log/log_app_test.cc \
        components/store/store_app_test.cc \
        components/store/kvstore_test.cc \
        components/store/versioned_kvstore_test.cc \
        components/store/hybrid_versioned_kvstore_test.cc \
        components/scheduler/scheduler_test.cc

PROTOS := components/log/log_reader.proto \
          components/store/kvstore.proto \
          components/store/versioned_kvstore.proto


DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(MACHINE_OBJS)

################################################################################

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-components: $(COMPONENTS_TESTS)
	@for a in $(COMPONENTS_TESTS); do \
    echo "";\
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
