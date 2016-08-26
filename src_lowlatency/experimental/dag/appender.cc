#include "experimental/dag/appender.h"

void Appender::Start() {
	
	
}

void Appender::Stop() {

}

// Call this within an infinite loop that keeps
// checking for new stickies. 
void Appender::Stickify(Sticky *s) {
	for (int i = 0; i < s->read_set_size(); ++i) {
		s->dependency_[i] = last_writers_[s->read_set[i]];
	}
	for (int i = 0; i < s->write_set_size(); ++i) {
		last_writers_[s->write_set[i]] = s;
	}
	
	// Check if the addition of this transaction causes the bound
	// on the maximum number of records read/written to be violated.
	std::set<Sticky*> transitive_closure;
	if (CheckCost(s, &transitive_closure) >= MAX_COST) {
		Materialize(&transitive_closure);
	}
}

// Materialize the transactions in the transitive closure. 
// For now just delete all the stickies. 
// XXX: We need to communicate which transactions to materialize to 
// the database. 
void Appender::Materialize(const std::set<Sticky*> *transitive_closure, 
						   const Sticky *s) {
	for (iterator i = transitive_closure->begin(); 
		 i < transitive_closure->end();
		 ++i) {
		delete(*i);
	}	
}

// Totally naive cost checker. It computes the *exact* cost
// of executing the transction. The the process is not incremental
// It is not incremental, it has to find the complete transitive closure
// every single time. 
//
// XXX: One thing we could do is just keep the transitive closures
// around in the last writers. 
int Appender::CheckCost(const Sticky *s, 
						std::set<Sticky*> *transitive_closure) {

	// The set of records we're going to have to process in some way. 
	std::set<int> records;

	// A queue of pending stickies to process in the BFS. 
	std::deque<Sticky*> to_process;
	to_process.insert(s);
	while (!(to_process.size() == 0)) {
		Sticky *cur = to_process.pop_front();
		for (int i = 0; i < cur->dependencies; ++i) {
			
			// Check that the dependency exists and hasn't yet been 
			// processed. If it hasn't, add it to the queue for 
			// processing. 
			if (cur->dependency_[i] && 
				(transitive_closure->count(cur->dependency_[i]) == 0)) {
				transitive_closure->insert(s);
				to_process.push_back(s);
			}
		}
	}
	
	// Find the actual records that we're going to be reading
	// and writing. 
	for (std::iterator it = transitive_closure->cbegin(); 
		 it < transitive_closure->cend();
		 ++it) {

		// Add all the records in the read set. 
		for (int i = 0; i < READ_SET_SIZE; ++i) 
			records.insert((*it)->read_set_[i]);
		
		// Add all the records in the write set. 
		for (int i = 0; i < WRITE_SET_SIZE; ++i)
			records.insert((*it)->write_set_[i]);
	}
	

	// Return the total number of records, which is an approximation
	// of the cost. 
	return records.count();
}
