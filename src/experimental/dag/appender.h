#ifndef CALVIN_COMPONENTS_DAG_APPENDER_H_
#define CALVIN_COMPONENTS_DAG_APPENDER_H_

#include <atomic>
#include "common/source.h"
#include "machine/machine.h"
#include "machine/app/app.h"

#define READ_SET_SIZE
#define WRITE_SET_SIZE

class Sticky;

class Appender : public App {
	
 public:	
 Appender(int latency) : 
	latency_bound_(latency), stickification_rq_(&no_actions_), 
		is_alive_(true) {

	}
	
	virtual ~Appender() {

	}	

	// Run infinitely, processing stickies as we go. 
	void Start() {
		Action *sticky;
		dead_ = false;
		while (is_alive_.load()) {
			if (stickification_rq_->Get(&sticky))
				Stickify(sticky);
		}			
	}
	
	// Stop the "start" thread. Spin until we're sure we're done. 
	void Stop() {
		is_alive_ = false;
		while (dead_.load())
			;
	}

	
 protected:				
	// Stickify a single transaction and check that the stickification
	// does not violate any the latency bound. 
	void Stickify(Sticky *s);

	// Atomic boolean to signal the stickification thread to stop. 
	std::atomic<bool> is_alive_;
	
	// Atomic boolean to signal the stop thread that we've actually
	// stopped. 
	std::atomic<bool> dead_; 
	
	// The latency bound in terms of the maximum number of records that we
	// are willing to read and write to execute a materializing read. 
	int latency_bound_;

	// Map the last writer of a given record so that we can
	// update the dependency graph quickly. 
	std::map<int, Stick*> last_writers_;
	
	// Default (empty) action request source. 
	EmptySource<Action*> no_actions_;

	// Pointer to the stream of transactions to be stickified. 
	Source<Action*>* stickification_rq_;
};

#endif
