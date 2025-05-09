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
#include <sys/select.h>  // For select() to handle pipe timeouts

#define PIPE_READ 0
#define PIPE_WRITE 1
#define CLOCK_MONOTONIC 1

// Constants for pipe communication
#define PIPE_BUFFER_SIZE 65536  // 64KB chunks
#define MAX_WRITE_RETRIES 10    // Maximum number of retry attempts
#define RETRY_DELAY_BASE_US 1000 // Base delay in microseconds (1ms)
#define DEFAULT_OUTPUT_FILE "primes.txt"  // Default output file name

typedef int pipe_t;

// Structure to store computation results
typedef struct {
    int *primeNumbers;     // Array to store found prime numbers
    size_t primeCount;     // Number of primes found
    char padding[64];      // Padding to prevent cache conflicts between threads
} calcInfo;

// Structure to share data between threads in a process
typedef struct {
    int *numbersToCheck;    // Range of numbers this process should check
    size_t totalNumbers;    // Total numbers to check in this process
    size_t nextIndex;       // Next number to check (shared index)
    sem_t semaphore;        // Semaphore for thread synchronization
    calcInfo *results;      // Pointer to results structure
} threadData;

// Global configuration variables
size_t thread_num;         // Number of threads per process
size_t subproc_num;        // Number of processes
int rangeStart;            // Start of the range to check
int rangeEnd;              // End of the range to check
size_t totalNumbers;       // Total number of numbers to check
char outputFile[256];      // Output file name

// Checks if a number is prime
bool isPrime(int num) {
    if (num <= 1) return false;
    if (num <= 3) return true;
    if (num % 2 == 0 || num % 3 == 0) return false;
    
    // Check using 6k +/- 1 optimization
    for (int i = 5; i * i <= num; i += 6) {
        if (num % i == 0 || num % (i + 2) == 0)
            return false;
    }
    return true;
}

// Initializes the result structure
void init_calc_info(calcInfo *info, size_t maxPrimes) {
    info->primeCount = 0;
    info->primeNumbers = (int*)malloc(maxPrimes * sizeof(int));
    if (info->primeNumbers == NULL) {
        perror("Memory allocation failed");
        exit(1);
    }
}

// Microsecond sleep function
void usleep_backoff(unsigned int microseconds) {
    struct timespec req, rem;
    req.tv_sec = microseconds / 1000000;
    req.tv_nsec = (microseconds % 1000000) * 1000;
    
    while (nanosleep(&req, &rem) == -1) {
        if (errno == EINTR) {
            // If interrupted by signal, sleep for remaining time
            req = rem;
        } else {
            break;
        }
    }
}

// Function executed by each thread
void *check_primes(void *arg) {
    threadData *data = (threadData *)arg;
    calcInfo *results = data->results;
    size_t localIndex;
    
    while (1) {
        // Get the next number to check (critical section)
        sem_wait(&data->semaphore);  // Wait on semaphore (lock)
        if (data->nextIndex >= data->totalNumbers) {
            sem_post(&data->semaphore);  // Release semaphore (unlock)
            break;  // No more numbers to check
        }
        localIndex = data->nextIndex++;
        sem_post(&data->semaphore);  // Release semaphore (unlock)
        
        // Check if the number is prime
        int numberToCheck = data->numbersToCheck[localIndex];
        if (isPrime(numberToCheck)) {
            // Found a prime, add it to results (critical section)
            sem_wait(&data->semaphore);  // Wait on semaphore (lock)
            results->primeNumbers[results->primeCount++] = numberToCheck;
            sem_post(&data->semaphore);  // Release semaphore (unlock)
        }
    }
    
    return NULL;
}

// Improved robust pipe write function with exponential backoff
ssize_t robust_write(int fd, const void *buf, size_t count) {
    size_t totalWritten = 0;
    const char *buffer = (const char *)buf;
    int retries = 0;
    
    while (totalWritten < count) {
        ssize_t result = write(fd, buffer + totalWritten, count - totalWritten);
        
        if (result > 0) {
            // Successful write
            totalWritten += result;
            retries = 0;  // Reset retry counter on success
        } else if (result == 0) {
            // No bytes written but no error
            // This is unusual for write, but let's add a small delay and try again
            retries++;
            
            // Exponential backoff with jitter
            unsigned int delay = RETRY_DELAY_BASE_US * (1 << (retries > 10 ? 10 : retries));
            // Add jitter (±20%)
            delay = delay * (80 + (rand() % 41)) / 100;
            
            usleep_backoff(delay);
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // Pipe buffer is full, implement backoff
            retries++;
            
            if (retries > MAX_WRITE_RETRIES) {
                fprintf(stderr, "Maximum write retries exceeded after %zu/%zu bytes\n", 
                        totalWritten, count);
                return totalWritten > 0 ? totalWritten : -1;
            }
            
            // Exponential backoff with jitter
            unsigned int delay = RETRY_DELAY_BASE_US * (1 << (retries > 10 ? 10 : retries));
            // Add jitter (±20%)
            delay = delay * (80 + (rand() % 41)) / 100;
            
            fprintf(stderr, "Pipe buffer full, retry %d in %u μs\n", retries, delay);
            usleep_backoff(delay);
        } else if (errno == EINTR) {
            // Interrupted by signal, just retry immediately
            continue;
        } else {
            // Other errors
            perror("Write error");
            return totalWritten > 0 ? totalWritten : -1;
        }
        
        // If we've tried many times but written some data, return what we've written
        if (retries > MAX_WRITE_RETRIES && totalWritten > 0) {
            return totalWritten;
        }
    }
    
    return totalWritten;
}

// Improved robust pipe read function with timeout
ssize_t robust_read(int fd, void *buf, size_t count, int timeout_seconds) {
    size_t totalRead = 0;
    char *buffer = (char *)buf;
    
    // Set up select timeout
    fd_set readfds;
    struct timeval tv;
    
    while (totalRead < count) {
        // Prepare for select
        FD_ZERO(&readfds);
        FD_SET(fd, &readfds);
        
        tv.tv_sec = timeout_seconds;
        tv.tv_usec = 0;
        
        // Wait until the pipe is ready for reading or timeout
        int ready = select(fd + 1, &readfds, NULL, NULL, &tv);
        
        if (ready == -1) {
            if (errno == EINTR) {
                // Interrupted by signal, retry
                continue;
            }
            perror("Select error");
            return totalRead > 0 ? totalRead : -1;
        } else if (ready == 0) {
            // Timeout occurred
            fprintf(stderr, "Read timeout after %zu/%zu bytes\n", totalRead, count);
            return totalRead > 0 ? totalRead : -1;
        }
        
        // Read is ready
        ssize_t result = read(fd, buffer + totalRead, count - totalRead);
        
        if (result > 0) {
            // Data read successfully
            totalRead += result;
        } else if (result == 0) {
            // End of file - pipe closed by writer
            break;
        } else if (errno == EINTR) {
            // Interrupted by signal, retry
            continue;
        } else {
            // Error
            perror("Read error");
            return totalRead > 0 ? totalRead : -1;
        }
    }
    
    return totalRead;
}

// Function called in the child process - creates threads and collects their results
void execute_fork(calcInfo *forkInfo, int *numbersToCheck, size_t totalNumbersForProcess) {
    pthread_t threads[thread_num];        // Array of thread identifiers
    
    // Create shared data for threads
    threadData sharedData;
    sharedData.numbersToCheck = numbersToCheck;
    sharedData.totalNumbers = totalNumbersForProcess;
    sharedData.nextIndex = 0;
    sharedData.results = forkInfo;
    
    // Initialize semaphore
    if (sem_init(&sharedData.semaphore, 0, 1) != 0) {
        perror("Semaphore initialization failed");
        exit(1);
    }
    
    printf("[Process %d] started with %zu threads, checking %zu numbers\n", 
           getpid(), thread_num, totalNumbersForProcess);
    
    // Create threads
    for (size_t i = 0; i < thread_num; i++) {
        pthread_create(&threads[i], NULL, check_primes, &sharedData);
    }
    
    // Wait for all threads to complete
    for (size_t i = 0; i < thread_num; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Destroy semaphore
    sem_destroy(&sharedData.semaphore);
    
    printf("[Process %d] found %zu prime numbers\n", getpid(), forkInfo->primeCount);
}

// Writes the primes to a file
void write_primes_to_file(int *primes, size_t count, const char *filename) {
    FILE *outfile = fopen(filename, "w");
    
    if (!outfile) {
        perror("Failed to open output file");
        return;
    }
    
    // First write the total count of primes
    fprintf(outfile, "Total prime numbers found: %zu\n\n", count);
    
    // Then write all the prime numbers
    for (size_t i = 0; i < count; i++) {
        fprintf(outfile, "%d\n", primes[i]);
    }
    
    fclose(outfile);
    printf("[MAIN] Prime numbers written to file: %s\n", filename);
}

// Parses program arguments and sets global variables
void read_args(int argc, char **argv) {
    // Check if we have 5 or 6 arguments
    if (argc != 5 && argc != 6) {
        fprintf(stderr, "Usage: %s <range_start> <range_end> <number_of_processes> <number_of_threads> [output_file]\n", argv[0]);
        exit(1);
    }
    
    // Convert input arguments to numbers
    rangeStart = atoi(argv[1]);
    rangeEnd = atoi(argv[2]);
    subproc_num = atoi(argv[3]);
    thread_num = atoi(argv[4]);
    
    // Set the output file name
    if (argc == 6) {
        strncpy(outputFile, argv[5], sizeof(outputFile) - 1);
        outputFile[sizeof(outputFile) - 1] = '\0';  // Ensure null termination
    } else {
        strncpy(outputFile, DEFAULT_OUTPUT_FILE, sizeof(outputFile) - 1);
    }
    
    // Calculate total numbers
    totalNumbers = rangeEnd - rangeStart + 1;
    
    // Validate input
    if (rangeStart < 0 || rangeEnd < rangeStart) {
        fprintf(stderr, "Invalid range: start must be >= 0 and end must be >= start\n");
        exit(1);
    }
    
    if (subproc_num < 1 || thread_num < 1) {
        fprintf(stderr, "Number of processes and threads must be at least 1\n");
        exit(1);
    }
}

// Returns the time in microseconds since the epoch - improved precision
long long get_time_us() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

// Displays the program's runtime parameters
void printProcessStatus() {
    printf("\nPROGRAM PARAMETERS\n");
    printf(" - Range: [%d, %d] (%zu numbers)\n", rangeStart, rangeEnd, totalNumbers);
    printf(" - Subprocesses: %zu\n", subproc_num);
    printf(" - Threads per subprocess: %zu\n", thread_num);
    printf(" - Output file: %s\n\n", outputFile);
}

// Main function of the program
int main(int argc, char **argv) {
    // Initialize random seed for jitter in backoff
    srand(time(NULL));
    
    read_args(argc, argv);        // Read input parameters
    
    long long timeStart = get_time_us();  // Measure start time with microsecond precision
    
    pid_t pid;
    pid_t child_pids[subproc_num];   // Array of child PIDs
    pipe_t pipes[subproc_num][2];    // Array of pipes for communication
    
    // Calculate work distribution - smarter division based on number size
    double *processBoundaries = (double*)malloc((subproc_num + 1) * sizeof(double));
    if (processBoundaries == NULL) {
        perror("Memory allocation failed");
        exit(1);
    }
    
    // Calculate the total "weight" of the range
    double totalWeight = sqrt(rangeEnd) - sqrt(rangeStart);
    double weightPerProcess = totalWeight / subproc_num;
    
    // Calculate boundaries for each process
    processBoundaries[0] = rangeStart;
    for (int i = 1; i < subproc_num; i++) {
        double targetSqrt = sqrt(rangeStart) + i * weightPerProcess;
        processBoundaries[i] = targetSqrt * targetSqrt;
    }
    processBoundaries[subproc_num] = rangeEnd + 1; // Upper bound (exclusive)
    
    // Create child processes
    for (int i = 0; i < subproc_num; i++) {
        if (pipe(pipes[i]) == -1) {  // Error check for pipe creation
            perror("Failed to create pipe");
            exit(1);
        }
        
        // Set pipe buffer size to a larger value if possible (Linux-specific)
        #ifdef F_SETPIPE_SZ
        long pipe_size = fcntl(pipes[i][PIPE_WRITE], F_GETPIPE_SZ);
        if (pipe_size != -1) {
            // Try to increase the pipe buffer size to 1MB if currently smaller
            if (pipe_size < 1048576) {
                fcntl(pipes[i][PIPE_WRITE], F_SETPIPE_SZ, 1048576);
            }
        }
        #endif
        
        // Ensure pipes are in blocking mode
        int flags = fcntl(pipes[i][PIPE_WRITE], F_GETFL);
        if (flags != -1) {
            // Clear O_NONBLOCK flag if it's set
            if (flags & O_NONBLOCK) {
                fcntl(pipes[i][PIPE_WRITE], F_SETFL, flags & ~O_NONBLOCK);
            }
        }
            
        pid = fork();              // Create a child process
        child_pids[i] = pid;
        
        if (pid == -1) {
            perror("Failed to create process");
            exit(1);
        } else if (pid == 0) {
            // Child process
            
            // Get range boundaries for this process
            int startNumber = (int)processBoundaries[i];
            int endNumber = (int)processBoundaries[i+1] - 1;
            size_t processNumbers = endNumber - startNumber + 1;
            
            // Create array of numbers for this process
            int *numbersToCheck = (int*)malloc(processNumbers * sizeof(int));
            if (numbersToCheck == NULL) {
                perror("Memory allocation failed");
                exit(1);
            }
            
            // Fill the array with numbers to check
            for (size_t j = 0; j < processNumbers; j++) {
                numbersToCheck[j] = startNumber + j;
            }
            
            printf("[Process %d] range: [%d-%d] (%zu numbers)\n", 
                   getpid(), startNumber, endNumber, processNumbers);
            
            // Initialize the results structure
            calcInfo localInfo;
            init_calc_info(&localInfo, processNumbers);  // Allocate max possible primes
            
            // Close read end of pipe in child as we only write
            close(pipes[i][PIPE_READ]);
            
            // Execute threads to check for primes
            execute_fork(&localInfo, numbersToCheck, processNumbers);
            
            // Send results to parent process
            // First write the count of primes
            if (robust_write(pipes[i][PIPE_WRITE], &localInfo.primeCount, sizeof(size_t)) != sizeof(size_t)) {
                fprintf(stderr, "[Process %d] Failed to write prime count\n", getpid());
                exit(1);
            }
            
            // Then write the prime numbers themselves in chunks to avoid pipe buffer limitations
            if (localInfo.primeCount > 0) {
                size_t bytesToWrite = localInfo.primeCount * sizeof(int);
                size_t bytesWritten = 0;
                char* buffer = (char*)localInfo.primeNumbers;
                
                // Write in smaller chunks
                while (bytesWritten < bytesToWrite) {
                    size_t currentChunk = bytesToWrite - bytesWritten;
                    if (currentChunk > PIPE_BUFFER_SIZE) {
                        currentChunk = PIPE_BUFFER_SIZE;
                    }
                    
                    ssize_t result = robust_write(pipes[i][PIPE_WRITE], 
                                               buffer + bytesWritten, 
                                               currentChunk);
                                               
                    if (result <= 0) {
                        fprintf(stderr, "[Process %d] Failed to write prime data after %zu/%zu bytes\n", 
                                getpid(), bytesWritten, bytesToWrite);
                        break;
                    }
                    
                    bytesWritten += result;
                    
                    // Small delay between chunks to let parent process read
                    if (bytesWritten < bytesToWrite) {
                        usleep_backoff(1000);  // 1ms delay
                    }
                }
                
                if (bytesWritten != bytesToWrite) {
                    fprintf(stderr, "[Process %d] Incomplete write: %zu of %zu bytes\n", 
                            getpid(), bytesWritten, bytesToWrite);
                }
            }
            
            // Make sure data is flushed before closing
            fsync(pipes[i][PIPE_WRITE]);
            close(pipes[i][PIPE_WRITE]);
            
            // Free allocated memory
            free(numbersToCheck);
            free(localInfo.primeNumbers);
            
            exit(0);  // Exit the child process
        } else {
            // Parent process - close write end immediately as we only read
            close(pipes[i][PIPE_WRITE]);
            
            // Ensure read pipe is in blocking mode
            int flags = fcntl(pipes[i][PIPE_READ], F_GETFL);
            if (flags != -1) {
                // Clear O_NONBLOCK flag if it's set
                if (flags & O_NONBLOCK) {
                    fcntl(pipes[i][PIPE_READ], F_SETFL, flags & ~O_NONBLOCK);
                }
            }
        }
    }
    
    // Parent process collects data from children
    // First determine the total number of primes found
    size_t totalPrimeCount = 0;
    size_t childCounts[subproc_num];
    
    for (int i = 0; i < subproc_num; i++) {
        childCounts[i] = 0;  // Initialize to zero
        
        // Read the count of primes found by this child with timeout
        size_t childPrimeCount;
        ssize_t readResult = robust_read(pipes[i][PIPE_READ], &childPrimeCount, sizeof(size_t), 5);
        
        if (readResult != sizeof(size_t)) {
            fprintf(stderr, "Failed to read prime count from child %d\n", i);
            continue;
        }
        
        childCounts[i] = childPrimeCount;
        totalPrimeCount += childPrimeCount;
        
        printf("[MAIN] Child %d reported %zu primes\n", i, childPrimeCount);
    }
    
    // Allocate memory for all prime numbers
    int *allPrimes = NULL;
    if (totalPrimeCount > 0) {
        allPrimes = (int*)malloc(totalPrimeCount * sizeof(int));
        if (allPrimes == NULL) {
            perror("Memory allocation failed");
            exit(1);
        }
    }
    
    // Now read the actual prime numbers
    size_t offset = 0;
    for (int i = 0; i < subproc_num; i++) {
        if (childCounts[i] > 0) {
            size_t bytesToRead = childCounts[i] * sizeof(int);
            char* buffer = (char*)(allPrimes + offset);
            
            // Read all prime numbers with timeout
            ssize_t bytesRead = robust_read(pipes[i][PIPE_READ], buffer, bytesToRead, 10);
            
            if (bytesRead != bytesToRead) {
                fprintf(stderr, "Incomplete read from child %d: %zd of %zu bytes\n", 
                        i, bytesRead, bytesToRead);
            } else {
                offset += childCounts[i];
            }
        }
        close(pipes[i][PIPE_READ]);
    }
    
    // Sort the primes in ascending order (optional - can be removed if not needed)
    if (totalPrimeCount > 0) {
        // Simple bubble sort - could be replaced with qsort for larger datasets
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
    
    // Write primes to file
    if (totalPrimeCount > 0) {
        write_primes_to_file(allPrimes, totalPrimeCount, outputFile);
    } else {
        printf("[MAIN] No prime numbers found to write to file.\n");
    }
    
    // Wait for all child processes to complete
    for (int i = 0; i < subproc_num; i++) {
        int status;
        pid_t result = waitpid(child_pids[i], &status, 0);
        
        if (result == -1) {
            perror("waitpid failed");
        } else if (WIFEXITED(status)) {
            if (WEXITSTATUS(status) != 0) {
                fprintf(stderr, "Child process %d exited with error code %d\n", 
                        child_pids[i], WEXITSTATUS(status));
            }
        } else if (WIFSIGNALED(status)) {
            fprintf(stderr, "Child process %d terminated by signal %d\n", 
                    child_pids[i], WTERMSIG(status));
        }
    }
    
    printProcessStatus();         // Display program parameters
    
    printf("[MAIN] finished with %zu subprocesses\n", subproc_num);
    printf("[MAIN] found %zu prime numbers in the range [%d, %d]\n", 
           totalPrimeCount, rangeStart, rangeEnd);
    
    long long timeEnd = get_time_us();  // Measure end time with microsecond precision
    
    double executionTimeSeconds = (timeEnd - timeStart)/1000000.0;  // Convert us to seconds
    double numbersPerSecond = totalNumbers / executionTimeSeconds;
    
    // Display the results with microsecond precision
    printf("[MAIN] Execution time: %.6f seconds (%lld μs)\n", 
           executionTimeSeconds, (timeEnd - timeStart));
    printf("[MAIN] Processing speed: %.2f numbers/sec\n", numbersPerSecond);
    
    // Free allocated memory
    free(allPrimes);
    free(processBoundaries);
    
    return 0;
}