// The following program probably contains a memory leak somewhere, making algorithms with more than 30 nodes to be
// delayed significantly.
// For bigger executions we recommend disabling the LOGS or just using 10-20 nodes max.

// Developed by: Dimitriadis Vasileios (2116104) & Taxiarchis Kouskouras (2116162)
// as part of a University Project (CS. Dept. University of Thessally) for Distributed Systems.

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>



/**
 * Sleep using ms (Taken from Stack Overflow)
 **/
#ifdef WIN32
#include <windows.h>
#elif _POSIX_C_SOURCE >= 199309L
#include <time.h>   // for nanosleep
#else
#include <unistd.h> // for usleep
#endif

void sleep_ms(int milliseconds) // cross-platform sleep function
{
#ifdef WIN32
    Sleep(milliseconds);
#elif _POSIX_C_SOURCE >= 199309L
    struct timespec ts;
    ts.tv_sec = milliseconds / 1000;
    ts.tv_nsec = (milliseconds % 1000) * 1000000;
    nanosleep(&ts, NULL);
#else
    usleep(milliseconds * 1000);
#endif
}

/**
 * END -Sleep using ms-
 **/
 
 
 
/**
 * GENERIC SETTINGS
 **/ 
#define DELAY 				 	10		// Pseudo Delay for nodes to simulate "Actual work" for each node.
#define LOG_STATE_CHANGES    	1		// Log the state changes for Debug purposes.
#define LOG_ORIENTATION_INFO 	1 		// Log the orientation info in the while loop for Debug purposes.
#define LOG_OTHER				1		// Log other actions for Debug purposes.
enum state_t {Internal=0, Send=1, Receive=2} state;

typedef struct node_token_t {
	int owner;
	int state;
	int succ;  // Successor (Next node).
	int pred;  // Predecessor (Previous node).
}Node_Token;

Node_Token *token_db;
int successor, predecessor;
typedef struct node_stats_t {
	int in_count_tokens,
		out_count_tokens;
}Node_Stats;

Node_Stats my_stats = {0, 0};

int rank, size;
char processor_name[MPI_MAX_PROCESSOR_NAME];
int processor_name_len;

void die(char *msg){
	fputs(msg, stderr);
	exit(1);
}

int get_prev(int rank, int size)
{
	return rank==0 ? size-1 : (rank-1)%size;
}

int random_int(int min, int max) // rand(min,max) => [min, max]
{
   return min + rand() % (max+1 - min);
}

void swap(int *src_a, int *src_b){
	int tmp = *src_a;
	*src_a = *src_b;
	*src_b = tmp;
}

void flip(){ // Just to name our function accordingly... nothing more, nothing less.
	swap(&successor, &predecessor);
}

/**
  * Custom printf to avoid Proccessor/Rank retyping
 **/
void print(char *fmt, ...){
	va_list argp;
	printf("[%s | %d] ", processor_name, rank);
	va_start(argp, fmt);
	vfprintf(stdout, fmt, argp);
	va_end(argp);
}

void debug_print(int show, char *fmt, ...){
	if(!show) return;
	va_list argp;
	printf("[%s | %d] ", processor_name, rank);
	va_start(argp, fmt);
	vfprintf(stdout, fmt, argp);
	va_end(argp);
}

Node_Token *generate_token(){
	Node_Token *token = malloc(sizeof(Node_Token));
	token->owner = rank;
	token->state = state;
	token->succ = successor;
	token->pred = predecessor;
	
	return token;
}

int get_orientation(){
	return successor == (rank+1)%size ? 1 : -1;
}

char *generate_orientation(){
	int MAX_BUF = 1024;
	char *msg = malloc(MAX_BUF*sizeof(char));
	int len=0;
	if(get_orientation() == 1){
		int i;
		for(i=0;i<size;i++){
			len += snprintf(msg+len, MAX_BUF-len, "%d->", (rank+i)%size);
		}
		len += snprintf(msg+len, MAX_BUF-len, "%d", rank);
	}else{
		int i;
		for(i=0;i<size;i++){
			int node_id = (rank - i) < 0 ? size - rank - i : rank-i;
			len += snprintf(msg+len, MAX_BUF-len, "%d->", node_id);
		}
		len += snprintf(msg+len, MAX_BUF-len, "%d", rank);
	}
	return msg;
}

const char *state_to_string(enum state_t st){
	if(st==0) return "Internal";
	else if(st==1) return "Send"; 
	else if(st==2) return "Receive"; 
}

/**
  * Basic setup for each Process
 **/
void setup(){
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // Rank of process || 0 to N-1 (from -np)
    MPI_Comm_size(MPI_COMM_WORLD, &size); // Size of world || N Processes (from  -np)
	
	// <= 0  Nodes means no Network
	//    1  Node means just a single PC => No Network
	//    2  Nodes are automatically directed ring (Each successor points to the other node necessarily)
	// As such, this algorithm works with 3+ nodes
	if(size <= 2) die("This algorithm works for 3+ nodes.\n");
	
	// Random Seed Generation (Unique to every node)
	srand(time(NULL)+rank);
	
	// Database of last_updated tokens (locally stored)
	token_db = (Node_Token *) malloc(size * sizeof(Node_Token));
	int i;
	for(i=0;i<size;i++){
		token_db[i].owner = i; 
		token_db[i].state = Internal; // All nodes are Internal by Default...
		token_db[i].pred = token_db[i].succ = -1; // We do not know their Successor/Predecessor before a token is received.
	}
	
    // Get the name of the processor
    MPI_Get_processor_name(processor_name, &processor_name_len);

	// Setup default values
	int dest = random_int(0, 1); // Randomizing successor & predecessor
	successor = (dest == 0 ? (rank+1)%size : get_prev(rank, size));
	predecessor = (dest == 0 ? get_prev(rank, size) : (rank+1)%size);
}

/**
  * MAIN FUNCTION
 **/
int main(int argc, char** argv) {
	MPI_Init(&argc, &argv);
    setup();
	
    Node_Token *token = NULL;
	MPI_Request request = MPI_REQUEST_NULL;
	MPI_Status status;
	
	// Generate random initial states
	if(rank == 0){ // Guarantee atleast one initial send state
		state = Send;
		token = generate_token();
	}else{ // And randomly select the next...
		state = random_int(0,2);
		if(state == Send) token = generate_token();
	}
	
	
	print("Successor = %d || Predecessor = %d || State = %s\n", successor, predecessor, state_to_string(state));
	
	int is_leader = 0; // The first node that finishes a complete loop becomes the "Leader".
	                   // This is only used to Synchronize and Debug the final statistics, and it is NOT REQUIRED for the ring algorith to work.
	int term_sequence = 0;
	do{
		int in_msg = 0;
		MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &in_msg, &status);
		if(state == Internal){
			if(in_msg){
				debug_print(LOG_STATE_CHANGES, "(1) Token Receive state (Forwarded by %d, to me %d)\n", status.MPI_SOURCE, rank);
				state = Receive;
				if(successor == status.MPI_SOURCE) flip();
			}else if(token_db[successor].state == Internal && token_db[successor].succ == rank){
				debug_print(LOG_STATE_CHANGES, "(5) Token Generation\n");
				token = generate_token();
				state = Send;
			}
		}
		if(state == Receive && in_msg){
			if(token) free(token); // Memory leak prevention
			token = malloc(sizeof(Node_Token));
			MPI_Recv(token, 4, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
			my_stats.in_count_tokens++;

			// - Termination Sequence -
			// Instead of breaking out of the loop here, we will brake at the end, to forward the token one last time.
			// As such, we create a chain-reaction of termination-sequences.
			// An alternative way is to "Listen" to a specific-tagged Async Message for termination.
			if(token->owner == rank && predecessor == status.MPI_SOURCE){ // Token has done a full circle back to its owner.
				debug_print(LOG_OTHER, "\n\n\n\n==========[RING ORIENTED]==========\n\n\n\n");
				term_sequence = 1;
			}else if(token_db[token->owner].state == token->state &&
					 token_db[token->owner].pred == token->pred &&
					 token_db[token->owner].succ == token->succ){ // Token has been re-received by the same node => Terminate.
				debug_print(LOG_OTHER, "\n\n\n\n==========[TERMINATION SEQUENCE]==========\n\n\n\n");
				term_sequence = 1;
			}
			
			token_db[token->owner] = *token; // Update token database.
		}
		
		if(state == Receive && token){
			debug_print(LOG_STATE_CHANGES, "(3) Spread the word (Token of %d, to %d)\n", token->owner, successor);
			state = Send;
		}
		
		if(state == Send){
			if(in_msg && successor == status.MPI_SOURCE){
				debug_print(LOG_STATE_CHANGES, "(4) Token Collision (Destroying my Token)\n");
				state = Receive;
				flip();
			}else if(token){
				MPI_Send(token, 4, MPI_INT, successor, 0, MPI_COMM_WORLD);
				my_stats.out_count_tokens++;
				free(token); // Cleanup memory since we forwarded the token
				token = NULL;
				state = Internal;
				debug_print(LOG_STATE_CHANGES, "(2) Token Transfer to %d\n", successor);
			}
		}
				
		char token_msg[128] = "No Token";
		if(token) snprintf(token_msg, 128, "Token: (O: %d, F: %d, M: %d)", token->owner, (in_msg ? status.MPI_SOURCE : -1), rank);
		debug_print(LOG_ORIENTATION_INFO, "Orientation: %d (%d) | State: %s | %s \n", get_orientation(), successor, state_to_string(state), token_msg);
				
		if(DELAY > 0) sleep_ms(DELAY + rank*100); // ~Randomize~ delay between nodes;
	}while(!term_sequence);
	
	MPI_Barrier(MPI_COMM_WORLD); // Synchronization || Wait for all nodes to finish the token exchanges. (Just to have a better final output)
	
	sleep_ms(1000+rank*100); // Delay a bit so we don't print messy.
	
	print("Successor: %d\n\t\tReceived %d tokens || Sent: %d tokens\n", successor, my_stats.in_count_tokens, my_stats.out_count_tokens);
	
	// - Collecting Final Statistics -
	// To collect the final statistics we need a leader. Here, lets just assume that leader is always the node 0.
	// The leader will complete the calculations necessary and present them finally to us.
	// IMPORTANT: We are changing the MESSAGE_TAG from 0 to 1, since we have some unsent messages at the 0 tag.
	//            An alternative way would be to clear out any remaining messages and then procceed to sending the statistics.
	// Alternative: An alternative way would be calling MPI's function ALL_GATHER since we are aware of who the leader/root is.
	if(rank == 0){
		Node_Stats final_stats = my_stats;
		Node_Stats *tmp = malloc(sizeof(Node_Stats));
		
		int i=size-1;
		for(i=1;i<size;i++){ // Skip Leader (Starting from 1 instead of 0)
			MPI_Recv(tmp, 2, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status); // Receive other node statistics.
			final_stats.in_count_tokens += tmp->in_count_tokens;
			final_stats.out_count_tokens += tmp->out_count_tokens;
		}
		char *orientation = generate_orientation();		
		sleep_ms(1000);
		printf("\n\n\n\n\n\n\n==========[Final Results - Leader : %d]==========\n\t\tOrientation: %d\n\t\tSequence: %s\n\t\tReceived %d tokens\n\t\tSent: %d tokens\n", rank, get_orientation(), generate_orientation(), final_stats.in_count_tokens, final_stats.out_count_tokens);
	}else{
		MPI_Isend(&my_stats, 2, MPI_INT, 0, 1, MPI_COMM_WORLD, &request); // Send my local stats to the leader
	}
	
	MPI_Barrier(MPI_COMM_WORLD);
	sleep_ms(10000);
	print("Goodbye!\n");
    MPI_Finalize();
}
