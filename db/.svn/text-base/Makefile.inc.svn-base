# Create template specific variables
UC_DIR := DB
LC_DIR := db

SRCS := db/client.cc
EXES :=

DEPS := $(COMMON_OBJS) $(PROTO_OBJS) $(MACHINE_OBJS) $(COMPONENTS_OBJS)

################################################################################

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-db: $(DB_TESTS)
	@for a in $(DB_TESTS); do \
    echo "";\
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
