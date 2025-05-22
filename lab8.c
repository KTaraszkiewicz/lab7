#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <stdbool.h>
#include <math.h>
#include <semaphore.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <signal.h>

#define PIPE_READ 0
#define PIPE_WRITE 1
#define FIFO_PATH "./zadania.in"
#define PIPE_BUFFER_SIZE 65536  // 64KB chunks
#define MAX_PATH 256
#define EXIT_COMMAND "EXIT"

// Debug logging macro
#define DEBUG_LOG(format, ...) \
    do { \
        fprintf(stderr, "[DEBUG %d] %s:%d - " format "\n", getpid(), __FUNCTION__, __LINE__, ##__VA_ARGS__); \
        fflush(stderr); \
    } while(0)

// Structure to store computation results
typedef struct {
    int *primeNumbers;     
    size_t primeCount;     
    char padding[64];      
} calcInfo;

// Structure to share data between threads in a process
typedef struct {
    int *numbersToCheck;    
    size_t totalNumbers;    
    size_t nextIndex;       
    sem_t semaphore;       
    calcInfo *results;      
} threadData;

// Structure for tasks
typedef struct {
    int rangeStart;
    int rangeEnd;
    char outputFile[MAX_PATH];
} task_t;

// Global variables
size_t thread_num;
size_t subproc_num;
pid_t *child_pids = NULL;
int **pipes_to_children = NULL;
int **pipes_from_children = NULL;
volatile sig_atomic_t terminate = 0;

// Signal handler for graceful termination
void handle_signal(int sig) {
    DEBUG_LOG("Signal %d received", sig);
    terminate = 1;
}

// Checks if a number is prime
bool isPrime(int num) {
    if (num <= 1) return false;
    if (num == 2) return true;
    if (num % 2 == 0) return false;
    if (num == 3) return true;
    if (num % 3 == 0) return false;
    
    for (int i = 5; i * i <= num; i += 6) {
        if (num % i == 0 || num % (i + 2) == 0)
            return false;
    }
    return true;
}

// Function executed by each thread
void *check_primes(void *arg) {
    DEBUG_LOG("Thread started");
    threadData *data = (threadData *)arg;
    calcInfo *results = data->results;
    size_t localIndex;
    int primes_found = 0;
    
    while (1) {
        // Get the next number to check
        if (sem_wait(&data->semaphore) != 0) {
            DEBUG_LOG("Thread sem_wait failed: %s", strerror(errno));
            break;
        }
        
        if (data->nextIndex >= data->totalNumbers) {
            sem_post(&data->semaphore);
            break;
        }
        localIndex = data->nextIndex++;
        sem_post(&data->semaphore);
        
        // Check if the number is prime
        int numberToCheck = data->numbersToCheck[localIndex];
        if (isPrime(numberToCheck)) {
            if (sem_wait(&data->semaphore) != 0) {
                DEBUG_LOG("Thread sem_wait failed in prime add: %s", strerror(errno));
                break;
            }
            results->primeNumbers[results->primeCount++] = numberToCheck;
            primes_found++;
            sem_post(&data->semaphore);
        }
    }
    
    DEBUG_LOG("Thread finished, found %d primes", primes_found);
    return NULL;
}

// Initialize result structure
void init_calc_info(calcInfo *info, size_t maxPrimes) {
    DEBUG_LOG("Initializing calcInfo for %zu max primes", maxPrimes);
    info->primeCount = 0;
    info->primeNumbers = (int*)calloc(maxPrimes, sizeof(int));  // Use calloc to zero-initialize
    if (info->primeNumbers == NULL) {
        DEBUG_LOG("Memory allocation failed for %zu primes", maxPrimes);
        perror("Memory allocation failed");
        exit(1);
    }
    DEBUG_LOG("calcInfo initialized successfully");
}

// Worker function for child processes
void child_process(int process_id) {
    DEBUG_LOG("Child process %d started", process_id);
    signal(SIGTERM, handle_signal);
    signal(SIGINT, handle_signal);
    
    // Close unnecessary pipe ends - keep only our communication pipes
    for (int i = 0; i < subproc_num; i++) {
        if (i != process_id) {
            if (pipes_to_children[i]) {
                close(pipes_to_children[i][PIPE_READ]);
                close(pipes_to_children[i][PIPE_WRITE]);
            }
            if (pipes_from_children[i]) {
                close(pipes_from_children[i][PIPE_READ]);
                close(pipes_from_children[i][PIPE_WRITE]);
            }
        }
    }
    
    // Close the write end of our input pipe and read end of our output pipe
    close(pipes_to_children[process_id][PIPE_WRITE]);
    close(pipes_from_children[process_id][PIPE_READ]);
    
    DEBUG_LOG("Child %d closed unnecessary pipes", process_id);
    
    int pipe_in = pipes_to_children[process_id][PIPE_READ];
    int pipe_out = pipes_from_children[process_id][PIPE_WRITE];
    
    DEBUG_LOG("Child %d using pipes: in=%d, out=%d", process_id, pipe_in, pipe_out);
    DEBUG_LOG("Child %d entering main loop", process_id);
    
    while (!terminate) {
        // Wait for task from parent
        task_t task;
        int cmd_type;
        
        DEBUG_LOG("Child %d waiting for command", process_id);
        ssize_t r = read(pipe_in, &cmd_type, sizeof(int));
        if (r <= 0) {
            if (r == 0) {
                DEBUG_LOG("Child %d pipe closed by parent", process_id);
            } else {
                DEBUG_LOG("Child %d read failed: %zd, errno: %s", process_id, r, strerror(errno));
            }
            break;
        }
        
        DEBUG_LOG("Child %d received command type: %d", process_id, cmd_type);
        
        if (cmd_type == 0) {
            // Exit command
            DEBUG_LOG("Child %d received exit command", process_id);
            break;
        } else if (cmd_type == 1) {
            // Prime calculation task
            DEBUG_LOG("Child %d reading task data", process_id);
            if (read(pipe_in, &task, sizeof(task_t)) <= 0) {
                DEBUG_LOG("Child %d failed to read task data", process_id);
                break;
            }
            
            DEBUG_LOG("Child %d processing range [%d-%d], threads: %zu", 
                     process_id, task.rangeStart, task.rangeEnd, thread_num);
            
            // Process range
            int start = task.rangeStart;
            int end = task.rangeEnd;
            size_t processNumbers = end - start + 1;
            
            DEBUG_LOG("Child %d allocating memory for %zu numbers", process_id, processNumbers);
            
            // Create array of numbers to check
            int *numbersToCheck = (int*)malloc(processNumbers * sizeof(int));
            if (numbersToCheck == NULL) {
                DEBUG_LOG("Child %d memory allocation failed", process_id);
                perror("Memory allocation failed");
                exit(1);
            }
            
            // Fill the array
            for (size_t j = 0; j < processNumbers; j++) {
                numbersToCheck[j] = start + j;
            }
            DEBUG_LOG("Child %d filled number array from %d to %d", process_id, numbersToCheck[0], numbersToCheck[processNumbers-1]);
            
            // Initialize results structure
            calcInfo localInfo;
            init_calc_info(&localInfo, processNumbers);
            
            // Set up thread data
            threadData sharedData;
            sharedData.numbersToCheck = numbersToCheck;
            sharedData.totalNumbers = processNumbers;
            sharedData.nextIndex = 0;
            sharedData.results = &localInfo;
            
            // Initialize semaphore
            DEBUG_LOG("Child %d initializing semaphore", process_id);
            if (sem_init(&sharedData.semaphore, 0, 1) != 0) {
                DEBUG_LOG("Child %d semaphore init failed: %s", process_id, strerror(errno));
                perror("Semaphore initialization failed");
                exit(1);
            }
            
            // Create threads
            DEBUG_LOG("Child %d creating %zu threads", process_id, thread_num);
            pthread_t *threads = malloc(thread_num * sizeof(pthread_t));
            if (threads == NULL) {
                DEBUG_LOG("Child %d thread allocation failed", process_id);
                perror("Thread allocation failed");
                exit(1);
            }
            
            for (size_t i = 0; i < thread_num; i++) {
                int result = pthread_create(&threads[i], NULL, check_primes, &sharedData);
                if (result != 0) {
                    DEBUG_LOG("Child %d pthread_create failed for thread %zu: %s", 
                             process_id, i, strerror(result));
                    exit(1);
                }
            }
            DEBUG_LOG("Child %d created all threads", process_id);
            
            // Wait for all threads to complete
            for (size_t i = 0; i < thread_num; i++) {
                int result = pthread_join(threads[i], NULL);
                if (result != 0) {
                    DEBUG_LOG("Child %d pthread_join failed for thread %zu: %s", 
                             process_id, i, strerror(result));
                }
            }
            DEBUG_LOG("Child %d all threads completed, found %zu primes", 
                     process_id, localInfo.primeCount);
            
            free(threads);
            sem_destroy(&sharedData.semaphore);
            
            // Send results to parent
            DEBUG_LOG("Child %d sending %zu primes to parent", process_id, localInfo.primeCount);
            if (write(pipe_out, &localInfo.primeCount, sizeof(size_t)) <= 0) {
                DEBUG_LOG("Child %d failed to write prime count", process_id);
            }
            
            if (localInfo.primeCount > 0) {
                ssize_t written = write(pipe_out, localInfo.primeNumbers, 
                                      localInfo.primeCount * sizeof(int));
                if (written <= 0) {
                    DEBUG_LOG("Child %d failed to write prime numbers", process_id);
                } else {
                    DEBUG_LOG("Child %d wrote %zd bytes of prime data", process_id, written);
                }
            }
            
            // Free resources
            free(numbersToCheck);
            free(localInfo.primeNumbers);
            DEBUG_LOG("Child %d task completed and resources freed", process_id);
        }
    }
    
    DEBUG_LOG("Child %d exiting", process_id);
    close(pipe_in);
    close(pipe_out);
    exit(0);
}

// Writes primes to a file
void write_primes_to_file(int *primes, size_t count, const char *filename) {
    DEBUG_LOG("Writing %zu primes to file: %s", count, filename);
    FILE *outfile = fopen(filename, "w");
    
    if (!outfile) {
        DEBUG_LOG("Failed to open output file: %s", filename);
        perror("Failed to open output file");
        return;
    }
    
    fprintf(outfile, "Total prime numbers found: %zu\n\n", count);
    
    for (size_t i = 0; i < count; i++) {
        // Skip any zeros that might have gotten in
        if (primes[i] > 0) {
            fprintf(outfile, "%d\n", primes[i]);
        } else {
            DEBUG_LOG("WARNING: Found zero or negative value at index %zu: %d", i, primes[i]);
        }
    }
    
    fclose(outfile);
    DEBUG_LOG("Successfully wrote primes to file: %s", filename);
}

// Process task in parent
void process_task(task_t *task) {
    DEBUG_LOG("Processing task: range [%d-%d], output: %s", 
             task->rangeStart, task->rangeEnd, task->outputFile);
    
    int rangeStart = task->rangeStart;
    int rangeEnd = task->rangeEnd;
    size_t totalNumbers = rangeEnd - rangeStart + 1;
    
    // Calculate work distribution
    DEBUG_LOG("Calculating work distribution for %zu numbers across %zu processes", 
             totalNumbers, subproc_num);
    
    double *processBoundaries = (double*)malloc((subproc_num + 1) * sizeof(double));
    if (processBoundaries == NULL) {
        DEBUG_LOG("Memory allocation failed for process boundaries");
        perror("Memory allocation failed");
        return;
    }
    
    // Use sqrt-based distribution to balance workload
    double totalWeight = sqrt(rangeEnd) - sqrt(rangeStart);
    double weightPerProcess = totalWeight / subproc_num;
    
    processBoundaries[0] = rangeStart;
    for (int i = 1; i < subproc_num; i++) {
        double targetSqrt = sqrt(rangeStart) + i * weightPerProcess;
        processBoundaries[i] = targetSqrt * targetSqrt;
    }
    processBoundaries[subproc_num] = rangeEnd + 1;
    
    // Send tasks to child processes
    DEBUG_LOG("Sending tasks to child processes");
    for (int i = 0; i < subproc_num; i++) {
        task_t subtask;
        subtask.rangeStart = (int)processBoundaries[i];
        subtask.rangeEnd = (int)processBoundaries[i+1] - 1;
        strncpy(subtask.outputFile, task->outputFile, MAX_PATH - 1);
        subtask.outputFile[MAX_PATH - 1] = '\0';
        
        DEBUG_LOG("Sending subtask to child %d: range [%d-%d]", 
                 i, subtask.rangeStart, subtask.rangeEnd);
        
        // Send command type first (1 = calculate primes)
        int cmd_type = 1;
        ssize_t w = write(pipes_to_children[i][PIPE_WRITE], &cmd_type, sizeof(int));
        if (w <= 0) {
            DEBUG_LOG("Failed to send command type to child %d: %s", i, strerror(errno));
            continue;
        }
        DEBUG_LOG("Sent command type to child %d", i);
        
        // Send the task
        w = write(pipes_to_children[i][PIPE_WRITE], &subtask, sizeof(task_t));
        if (w <= 0) {
            DEBUG_LOG("Failed to send task to child %d: %s", i, strerror(errno));
            continue;
        }
        DEBUG_LOG("Sent task to child %d", i);
    }
    
    // Collect results from child processes
    DEBUG_LOG("Collecting results from child processes");
    size_t totalPrimeCount = 0;
    size_t *childCounts = (size_t*)calloc(subproc_num, sizeof(size_t));
    
    for (int i = 0; i < subproc_num; i++) {
        size_t childPrimeCount = 0;
        DEBUG_LOG("Reading prime count from child %d", i);
        ssize_t r = read(pipes_from_children[i][PIPE_READ], &childPrimeCount, sizeof(size_t));
        if (r <= 0) {
            DEBUG_LOG("Failed to read prime count from child %d: %zd, errno: %s", i, r, strerror(errno));
            childPrimeCount = 0;
        }
        childCounts[i] = childPrimeCount;
        totalPrimeCount += childPrimeCount;
        DEBUG_LOG("Child %d reported %zu primes", i, childPrimeCount);
    }
    
    DEBUG_LOG("Total primes from all children: %zu", totalPrimeCount);
    
    // Allocate memory for all primes
    int *allPrimes = NULL;
    if (totalPrimeCount > 0) {
        DEBUG_LOG("Allocating memory for %zu total primes", totalPrimeCount);
        allPrimes = (int*)calloc(totalPrimeCount, sizeof(int));  // Use calloc to zero-initialize
        if (allPrimes == NULL) {
            DEBUG_LOG("Memory allocation failed for all primes");
            perror("Memory allocation failed");
            free(childCounts);
            free(processBoundaries);
            return;
        }
    }
    
    // Read the actual prime numbers
    DEBUG_LOG("Reading prime numbers from children");
    size_t offset = 0;
    for (int i = 0; i < subproc_num; i++) {
        if (childCounts[i] > 0) {
            ssize_t expected_bytes = childCounts[i] * sizeof(int);
            DEBUG_LOG("Reading %zu primes (%zd bytes) from child %d", childCounts[i], expected_bytes, i);
            ssize_t r = read(pipes_from_children[i][PIPE_READ], 
                           allPrimes + offset, expected_bytes);
            if (r != expected_bytes) {
                DEBUG_LOG("Child %d: expected %zd bytes, got %zd bytes", 
                         i, expected_bytes, r);
            } else {
                DEBUG_LOG("Successfully read %zu primes from child %d", childCounts[i], i);
            }
            offset += childCounts[i];
        }
    }
    
    // Sort primes (simple bubble sort)
    if (totalPrimeCount > 1) {
        DEBUG_LOG("Sorting %zu primes", totalPrimeCount);
        for (size_t i = 0; i < totalPrimeCount - 1; i++) {
            for (size_t j = 0; j < totalPrimeCount - i - 1; j++) {
                if (allPrimes[j] > allPrimes[j + 1]) {
                    int temp = allPrimes[j];
                    allPrimes[j] = allPrimes[j + 1];
                    allPrimes[j + 1] = temp;
                }
            }
        }
        DEBUG_LOG("Sorting completed");
    }
    
    // Write results to file
    if (totalPrimeCount > 0) {
        // Debug: print first few primes to check for zeros
        DEBUG_LOG("First 10 primes to be written:");
        for (size_t i = 0; i < totalPrimeCount && i < 10; i++) {
            DEBUG_LOG("  Prime[%zu] = %d", i, allPrimes[i]);
        }
        write_primes_to_file(allPrimes, totalPrimeCount, task->outputFile);
    } else {
        // Create empty output file with zero primes message
        DEBUG_LOG("No primes found, creating empty output file");
        FILE *outfile = fopen(task->outputFile, "w");
        if (outfile) {
            fprintf(outfile, "Total prime numbers found: 0\n\n");
            fclose(outfile);
        }
    }
    
    // Free resources
    free(allPrimes);
    free(childCounts);
    free(processBoundaries);
    DEBUG_LOG("Task processing completed successfully");
}

// Clean up resources
void cleanup() {
    DEBUG_LOG("Starting cleanup");
    
    // Send termination command to all child processes
    if (child_pids && pipes_to_children) {
        DEBUG_LOG("Sending termination commands to children");
        for (int i = 0; i < subproc_num; i++) {
            if (child_pids[i] > 0) {
                int cmd_type = 0;  // 0 = exit
                ssize_t w = write(pipes_to_children[i][PIPE_WRITE], &cmd_type, sizeof(int));
                if (w <= 0) {
                    DEBUG_LOG("Failed to send exit command to child %d: %s", i, strerror(errno));
                } else {
                    DEBUG_LOG("Sent exit command to child %d", i);
                }
            }
        }
        
        // Give children time to process exit command
        usleep(100000);  // 100ms
        
        // Wait for children to terminate
        DEBUG_LOG("Waiting for children to terminate");
        for (int i = 0; i < subproc_num; i++) {
            if (child_pids[i] > 0) {
                int status;
                pid_t result = waitpid(child_pids[i], &status, WNOHANG);
                if (result == 0) {
                    // Child still running, send SIGTERM
                    DEBUG_LOG("Child %d still running, sending SIGTERM", i);
                    kill(child_pids[i], SIGTERM);
                    result = waitpid(child_pids[i], &status, 0);
                }
                
                if (result == child_pids[i]) {
                    DEBUG_LOG("Child %d (PID %d) terminated with status %d", i, child_pids[i], status);
                } else {
                    DEBUG_LOG("Failed to wait for child %d (PID %d): %s", i, child_pids[i], strerror(errno));
                }
            }
        }
    }
    
    // Close all pipes
    if (pipes_to_children) {
        DEBUG_LOG("Closing pipes to children");
        for (int i = 0; i < subproc_num; i++) {
            if (pipes_to_children[i]) {
                close(pipes_to_children[i][PIPE_WRITE]);
                free(pipes_to_children[i]);
            }
        }
        free(pipes_to_children);
    }
    
    if (pipes_from_children) {
        DEBUG_LOG("Closing pipes from children");
        for (int i = 0; i < subproc_num; i++) {
            if (pipes_from_children[i]) {
                close(pipes_from_children[i][PIPE_READ]);
                free(pipes_from_children[i]);
            }
        }
        free(pipes_from_children);
    }
    
    free(child_pids);
    
    // Remove the named pipe
    DEBUG_LOG("Removing FIFO");
    unlink(FIFO_PATH);
    
    DEBUG_LOG("Cleanup completed");
}

// Read a line from FIFO with proper handling of EOF and reopening
ssize_t read_line_from_fifo(char *buffer, size_t buffer_size) {
    static int fifo_fd = -1;
    ssize_t total_read = 0;
    char ch;
    
    DEBUG_LOG("Reading line from FIFO (fd: %d)", fifo_fd);
    
    while (!terminate) {
        // Open FIFO if not already open
        if (fifo_fd == -1) {
            DEBUG_LOG("Opening FIFO: %s", FIFO_PATH);
            fifo_fd = open(FIFO_PATH, O_RDONLY);
            if (fifo_fd == -1) {
                if (errno == EINTR && terminate) break;
                DEBUG_LOG("Failed to open FIFO: %s", strerror(errno));
                usleep(100000); // Wait 100ms before retry
                continue;
            }
            DEBUG_LOG("FIFO opened successfully, fd: %d", fifo_fd);
        }
        
        // Try to read one character
        ssize_t bytes_read = read(fifo_fd, &ch, 1);
        
        if (bytes_read == 1) {
            // Successfully read a character
            if (ch == '\n' || total_read >= buffer_size - 1) {
                // End of line or buffer full
                buffer[total_read] = '\0';
                DEBUG_LOG("Read complete line: '%s' (%zd chars)", buffer, total_read);
                return total_read;
            } else {
                // Add character to buffer
                buffer[total_read++] = ch;
            }
        } else if (bytes_read == 0) {
            // EOF - writer closed their end
            DEBUG_LOG("EOF detected on FIFO, will reopen for next command");
            close(fifo_fd);
            fifo_fd = -1;
            
            // If we have data in buffer, return it
            if (total_read > 0) {
                buffer[total_read] = '\0';
                DEBUG_LOG("Returning partial line: '%s' (%zd chars)", buffer, total_read);
                return total_read;
            }
            
            // Reset buffer and continue to reopen FIFO for next command
            total_read = 0;
            continue;
        } else {
            // Error reading
            if (errno == EINTR && terminate) break;
            DEBUG_LOG("Error reading from FIFO: %s", strerror(errno));
            close(fifo_fd);
            fifo_fd = -1;
            usleep(100000); // Wait before retry
        }
    }
    
    // Clean up on exit
    if (fifo_fd != -1) {
        DEBUG_LOG("Closing FIFO on exit");
        close(fifo_fd);
    }
    
    return -1;
}

int main(int argc, char **argv) {
    DEBUG_LOG("Program started with PID %d", getpid());
    
    // Check arguments
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <num_processes> <num_threads>\n", argv[0]);
        return 1;
    }
    
    // Parse input arguments
    subproc_num = atoi(argv[1]);
    thread_num = atoi(argv[2]);
    
    DEBUG_LOG("Configuration: %zu processes, %zu threads per process", subproc_num, thread_num);
    
    if (subproc_num < 1 || thread_num < 1) {
        fprintf(stderr, "Number of processes and threads must be at least 1\n");
        return 1;
    }
    
    // Set up signal handlers
    DEBUG_LOG("Setting up signal handlers");
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);  // Ignore broken pipe signals
    
    // Create named pipe (FIFO)
    DEBUG_LOG("Creating FIFO: %s", FIFO_PATH);
    unlink(FIFO_PATH);  // Remove if exists
    if (mkfifo(FIFO_PATH, 0666) == -1) {
        DEBUG_LOG("Failed to create FIFO: %s", strerror(errno));
        perror("Failed to create FIFO");
        return 1;
    }
    DEBUG_LOG("FIFO created successfully");
    
    // Allocate memory for child PIDs and pipes
    DEBUG_LOG("Allocating memory for %zu processes", subproc_num);
    child_pids = (pid_t*)calloc(subproc_num, sizeof(pid_t));
    pipes_to_children = (int**)calloc(subproc_num, sizeof(int*));
    pipes_from_children = (int**)calloc(subproc_num, sizeof(int*));
    
    if (!child_pids || !pipes_to_children || !pipes_from_children) {
        DEBUG_LOG("Memory allocation failed for process structures");
        perror("Memory allocation failed");
        cleanup();
        return 1;
    }
    
    // Create pipes and child processes
    DEBUG_LOG("Creating pipes and child processes");
    for (int i = 0; i < subproc_num; i++) {
        DEBUG_LOG("Creating process %d", i);
        
        pipes_to_children[i] = (int*)malloc(2 * sizeof(int));
        pipes_from_children[i] = (int*)malloc(2 * sizeof(int));
        
        if (!pipes_to_children[i] || !pipes_from_children[i]) {
            DEBUG_LOG("Pipe allocation failed for process %d", i);
            perror("Pipe allocation failed");
            cleanup();
            return 1;
        }
        
        if (pipe(pipes_to_children[i]) == -1 || pipe(pipes_from_children[i]) == -1) {
            DEBUG_LOG("Failed to create pipes for process %d: %s", i, strerror(errno));
            perror("Failed to create pipe");
            cleanup();
            return 1;
        }
        
        DEBUG_LOG("Created pipes for process %d: to_child(%d,%d), from_child(%d,%d)", 
                 i, pipes_to_children[i][0], pipes_to_children[i][1], 
                 pipes_from_children[i][0], pipes_from_children[i][1]);
        
        pid_t pid = fork();
        
        if (pid == -1) {
            DEBUG_LOG("Failed to create process %d: %s", i, strerror(errno));
            perror("Failed to create process");
            cleanup();
            return 1;
        } else if (pid == 0) {
            // Child process
            DEBUG_LOG("Child process %d starting", i);
            child_process(i);
            exit(0);  // Should not reach here
        } else {
            child_pids[i] = pid;
            DEBUG_LOG("Created child process %d with PID %d", i, pid);
        }
    }
    
    // Parent process - close unnecessary pipe ends
    DEBUG_LOG("Parent closing unnecessary pipe ends");
    for (int i = 0; i < subproc_num; i++) {
        close(pipes_to_children[i][PIPE_READ]);
        close(pipes_from_children[i][PIPE_WRITE]);
    }
    
    printf("Prime number finder started. Listening for tasks on %s\n", FIFO_PATH);
    printf("Use format: <start> <end> <output_file> or type EXIT to quit\n");
    
    DEBUG_LOG("Entering main loop");
    
    // Main loop - read tasks from FIFO with proper handling
    char buffer[256];
    while (!terminate) {
        DEBUG_LOG("Waiting for next task");
        ssize_t bytes_read = read_line_from_fifo(buffer, sizeof(buffer));
        
        if (bytes_read < 0) {
            // Error or termination
            DEBUG_LOG("read_line_from_fifo returned error");
            break;
        }
        
        if (bytes_read == 0) {
            // Empty line, continue
            DEBUG_LOG("Empty line received, continuing");
            continue;
        }
        
        // Remove trailing whitespace
        while (bytes_read > 0 && (buffer[bytes_read - 1] == '\n' || buffer[bytes_read - 1] == '\r' || buffer[bytes_read - 1] == ' ')) {
            buffer[--bytes_read] = '\0';
        }
        
        if (bytes_read == 0) {
            // Line was just whitespace
            DEBUG_LOG("Line was just whitespace, continuing");
            continue;
        }
        
        DEBUG_LOG("Received command: '%s'", buffer);
        
        // Check for EXIT command
        if (strcmp(buffer, EXIT_COMMAND) == 0) {
            printf("EXIT command received, shutting down...\n");
            DEBUG_LOG("EXIT command received");
            break;
        }
        
        // Parse task
        task_t task;
        if (sscanf(buffer, "%d %d %255s", &task.rangeStart, &task.rangeEnd, task.outputFile) == 3) {
            printf("Processing task: range [%d-%d], output: %s\n", 
                   task.rangeStart, task.rangeEnd, task.outputFile);
                   
            process_task(&task);
            
            printf("Task completed, results written to %s\n", task.outputFile);
        } else {
            fprintf(stderr, "Invalid task format. Use: <start> <end> <output_file>\n");
            fprintf(stderr, "Received: '%s'\n", buffer);
            DEBUG_LOG("Invalid task format received: '%s'", buffer);
        }
    }
    
    printf("Shutting down processes...\n");
    DEBUG_LOG("Main loop ended, starting cleanup");
    cleanup();
    printf("All processes terminated.\n");
    DEBUG_LOG("Program terminating normally");
    
    return 0;
}