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
    terminate = 1;
}

// Checks if a number is prime
bool isPrime(int num) {
    if (num <= 1) return false;
    if (num <= 3) return true;
    if (num % 2 == 0 || num % 3 == 0) return false;
    
    for (int i = 5; i * i <= num; i += 6) {
        if (num % i == 0 || num % (i + 2) == 0)
            return false;
    }
    return true;
}

// Function executed by each thread
void *check_primes(void *arg) {
    threadData *data = (threadData *)arg;
    calcInfo *results = data->results;
    size_t localIndex;
    
    while (1) {
        // Get the next number to check
        sem_wait(&data->semaphore);
        if (data->nextIndex >= data->totalNumbers) {
            sem_post(&data->semaphore);
            break;
        }
        localIndex = data->nextIndex++;
        sem_post(&data->semaphore);
        
        // Check if the number is prime
        int numberToCheck = data->numbersToCheck[localIndex];
        if (isPrime(numberToCheck)) {
            sem_wait(&data->semaphore);
            results->primeNumbers[results->primeCount++] = numberToCheck;
            sem_post(&data->semaphore);
        }
    }
    
    return NULL;
}

// Initialize result structure
void init_calc_info(calcInfo *info, size_t maxPrimes) {
    info->primeCount = 0;
    info->primeNumbers = (int*)malloc(maxPrimes * sizeof(int));
    if (info->primeNumbers == NULL) {
        perror("Memory allocation failed");
        exit(1);
    }
}

// Worker function for child processes
void child_process(int process_id) {
    signal(SIGTERM, handle_signal);
    
    // Close unnecessary pipe ends
    for (int i = 0; i < subproc_num; i++) {
        if (i != process_id) {
            close(pipes_to_children[i][PIPE_READ]);
            close(pipes_from_children[i][PIPE_WRITE]);
        }
    }
    
    int pipe_in = pipes_to_children[process_id][PIPE_READ];
    int pipe_out = pipes_from_children[process_id][PIPE_WRITE];
    
    while (!terminate) {
        // Wait for task from parent
        task_t task;
        int cmd_type;
        
        ssize_t r = read(pipe_in, &cmd_type, sizeof(int));
        if (r <= 0) break;
        
        if (cmd_type == 0) {
            // Exit command
            break;
        } else if (cmd_type == 1) {
            // Prime calculation task
            if (read(pipe_in, &task, sizeof(task_t)) <= 0) break;
            
            // Process range
            int start = task.rangeStart;
            int end = task.rangeEnd;
            size_t processNumbers = end - start + 1;
            
            // Create array of numbers to check
            int *numbersToCheck = (int*)malloc(processNumbers * sizeof(int));
            if (numbersToCheck == NULL) {
                perror("Memory allocation failed");
                exit(1);
            }
            
            // Fill the array
            for (size_t j = 0; j < processNumbers; j++) {
                numbersToCheck[j] = start + j;
            }
            
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
            if (sem_init(&sharedData.semaphore, 0, 1) != 0) {
                perror("Semaphore initialization failed");
                exit(1);
            }
            
            // Create threads
            pthread_t *threads = malloc(thread_num * sizeof(pthread_t));
            if (threads == NULL) {
                perror("Thread allocation failed");
                exit(1);
            }
            
            for (size_t i = 0; i < thread_num; i++) {
                pthread_create(&threads[i], NULL, check_primes, &sharedData);
            }
            
            // Wait for all threads to complete
            for (size_t i = 0; i < thread_num; i++) {
                pthread_join(threads[i], NULL);
            }
            
            free(threads);
            sem_destroy(&sharedData.semaphore);
            
            // Send results to parent
            write(pipe_out, &localInfo.primeCount, sizeof(size_t));
            
            if (localInfo.primeCount > 0) {
                write(pipe_out, localInfo.primeNumbers, 
                      localInfo.primeCount * sizeof(int));
            }
            
            // Free resources
            free(numbersToCheck);
            free(localInfo.primeNumbers);
        }
    }
    
    close(pipe_in);
    close(pipe_out);
    exit(0);
}

// Writes primes to a file
void write_primes_to_file(int *primes, size_t count, const char *filename) {
    FILE *outfile = fopen(filename, "w");
    
    if (!outfile) {
        perror("Failed to open output file");
        return;
    }
    
    fprintf(outfile, "Total prime numbers found: %zu\n\n", count);
    
    for (size_t i = 0; i < count; i++) {
        fprintf(outfile, "%d\n", primes[i]);
    }
    
    fclose(outfile);
}

// Process task in parent
void process_task(task_t *task) {
    int rangeStart = task->rangeStart;
    int rangeEnd = task->rangeEnd;
    size_t totalNumbers = rangeEnd - rangeStart + 1;
    
    // Calculate work distribution
    double *processBoundaries = (double*)malloc((subproc_num + 1) * sizeof(double));
    if (processBoundaries == NULL) {
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
    for (int i = 0; i < subproc_num; i++) {
        task_t subtask;
        subtask.rangeStart = (int)processBoundaries[i];
        subtask.rangeEnd = (int)processBoundaries[i+1] - 1;
        strncpy(subtask.outputFile, task->outputFile, MAX_PATH - 1);
        subtask.outputFile[MAX_PATH - 1] = '\0';
        
        // Send command type first (1 = calculate primes)
        int cmd_type = 1;
        write(pipes_to_children[i][PIPE_WRITE], &cmd_type, sizeof(int));
        
        // Send the task
        write(pipes_to_children[i][PIPE_WRITE], &subtask, sizeof(task_t));
    }
    
    // Collect results from child processes
    size_t totalPrimeCount = 0;
    size_t *childCounts = (size_t*)malloc(subproc_num * sizeof(size_t));
    
    for (int i = 0; i < subproc_num; i++) {
        size_t childPrimeCount = 0;
        read(pipes_from_children[i][PIPE_READ], &childPrimeCount, sizeof(size_t));
        childCounts[i] = childPrimeCount;
        totalPrimeCount += childPrimeCount;
    }
    
    // Allocate memory for all primes
    int *allPrimes = NULL;
    if (totalPrimeCount > 0) {
        allPrimes = (int*)malloc(totalPrimeCount * sizeof(int));
        if (allPrimes == NULL) {
            perror("Memory allocation failed");
            free(childCounts);
            free(processBoundaries);
            return;
        }
    }
    
    // Read the actual prime numbers
    size_t offset = 0;
    for (int i = 0; i < subproc_num; i++) {
        if (childCounts[i] > 0) {
            read(pipes_from_children[i][PIPE_READ], 
                 allPrimes + offset, 
                 childCounts[i] * sizeof(int));
            offset += childCounts[i];
        }
    }
    
    // Sort primes (simple bubble sort)
    if (totalPrimeCount > 0) {
        for (size_t i = 0; i < totalPrimeCount - 1; i++) {
            for (size_t j = 0; j < totalPrimeCount - i - 1; j++) {
                if (allPrimes[j] > allPrimes[j + 1]) {
                    int temp = allPrimes[j];
                    allPrimes[j] = allPrimes[j + 1];
                    allPrimes[j + 1] = temp;
                }
            }
        }
    }
    
    // Write results to file
    if (totalPrimeCount > 0) {
        write_primes_to_file(allPrimes, totalPrimeCount, task->outputFile);
    }
    
    // Free resources
    free(allPrimes);
    free(childCounts);
    free(processBoundaries);
}

// Clean up resources
void cleanup() {
    // Send termination command to all child processes
    if (child_pids && pipes_to_children) {
        for (int i = 0; i < subproc_num; i++) {
            if (child_pids[i] > 0) {
                int cmd_type = 0;  // 0 = exit
                write(pipes_to_children[i][PIPE_WRITE], &cmd_type, sizeof(int));
            }
        }
        
        // Wait for children to terminate
        for (int i = 0; i < subproc_num; i++) {
            if (child_pids[i] > 0) {
                waitpid(child_pids[i], NULL, 0);
            }
        }
    }
    
    // Close all pipes
    if (pipes_to_children) {
        for (int i = 0; i < subproc_num; i++) {
            close(pipes_to_children[i][PIPE_WRITE]);
            free(pipes_to_children[i]);
        }
        free(pipes_to_children);
    }
    
    if (pipes_from_children) {
        for (int i = 0; i < subproc_num; i++) {
            close(pipes_from_children[i][PIPE_READ]);
            free(pipes_from_children[i]);
        }
        free(pipes_from_children);
    }
    
    free(child_pids);
    
    // Remove the named pipe
    unlink(FIFO_PATH);
}

int main(int argc, char **argv) {
    // Check arguments
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <num_processes> <num_threads>\n", argv[0]);
        return 1;
    }
    
    // Parse input arguments
    subproc_num = atoi(argv[1]);
    thread_num = atoi(argv[2]);
    
    if (subproc_num < 1 || thread_num < 1) {
        fprintf(stderr, "Number of processes and threads must be at least 1\n");
        return 1;
    }
    
    // Set up signal handlers
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    
    // Create named pipe (FIFO)
    unlink(FIFO_PATH);  // Remove if exists
    if (mkfifo(FIFO_PATH, 0666) == -1) {
        perror("Failed to create FIFO");
        return 1;
    }
    
    // Allocate memory for child PIDs and pipes
    child_pids = (pid_t*)malloc(subproc_num * sizeof(pid_t));
    pipes_to_children = (int**)malloc(subproc_num * sizeof(int*));
    pipes_from_children = (int**)malloc(subproc_num * sizeof(int*));
    
    if (!child_pids || !pipes_to_children || !pipes_from_children) {
        perror("Memory allocation failed");
        cleanup();
        return 1;
    }
    
    // Create pipes and child processes
    for (int i = 0; i < subproc_num; i++) {
        pipes_to_children[i] = (int*)malloc(2 * sizeof(int));
        pipes_from_children[i] = (int*)malloc(2 * sizeof(int));
        
        if (!pipes_to_children[i] || !pipes_from_children[i]) {
            perror("Pipe allocation failed");
            cleanup();
            return 1;
        }
        
        if (pipe(pipes_to_children[i]) == -1 || pipe(pipes_from_children[i]) == -1) {
            perror("Failed to create pipe");
            cleanup();
            return 1;
        }
        
        pid_t pid = fork();
        child_pids[i] = pid;
        
        if (pid == -1) {
            perror("Failed to create process");
            cleanup();
            return 1;
        } else if (pid == 0) {
            // Child process
            child_process(i);
            exit(0);  // Should not reach here
        }
    }
    
    // Parent process - close unnecessary pipe ends
    for (int i = 0; i < subproc_num; i++) {
        close(pipes_to_children[i][PIPE_READ]);
        close(pipes_from_children[i][PIPE_WRITE]);
    }
    
    printf("Prime number finder started. Listening for tasks on %s\n", FIFO_PATH);
    printf("Use format: <start> <end> <output_file> or type EXIT to quit\n");
    
    // Main loop - read tasks from FIFO
    while (!terminate) {
        int fifo_fd = open(FIFO_PATH, O_RDONLY);
        if (fifo_fd == -1) {
            if (errno == EINTR && terminate) break;
            perror("Failed to open FIFO");
            continue;
        }
        
        char buffer[256];
        ssize_t bytes_read = read(fifo_fd, buffer, sizeof(buffer) - 1);
        close(fifo_fd);
        
        if (bytes_read <= 0) {
            if (errno == EINTR && terminate) break;
            continue;
        }
        
        // Null-terminate the buffer
        buffer[bytes_read] = '\0';
        
        // Remove trailing newline if present
        if (buffer[bytes_read - 1] == '\n') {
            buffer[bytes_read - 1] = '\0';
        }
        
        // Check for EXIT command
        if (strcmp(buffer, EXIT_COMMAND) == 0) {
            printf("EXIT command received, shutting down...\n");
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
        }
    }
    
    printf("Shutting down processes...\n");
    cleanup();
    printf("All processes terminated.\n");
    
    return 0;
}
