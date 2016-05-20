# Create template specific variables
UC_DIR := MACHINE
LC_DIR := machine

SRCS := machine/app/app.cc \
        machine/app/testapp.cc \
        machine/app/reporter.cc \
        machine/cluster_config.cc \
        machine/machine.cc \
        machine/connection/connection_zmq.cc \
        machine/thread_pool/thread_pool.cc \
        machine/cluster_manager.cc \
        machine/external_connection.cc
EXES :=
TEST := machine/cluster_config_test.cc \
        machine/machine_test.cc \
        machine/app/app_test.cc \
        machine/connection/connection_zmq_test.cc \
        machine/thread_pool/thread_pool_test.cc

DEPS := $(COMMON_OBJS) $(PROTO_OBJS)

################################################################################

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-machine: $(MACHINE_TESTS)
	@for a in $(MACHINE_TESTS); do \
    echo "";\
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
