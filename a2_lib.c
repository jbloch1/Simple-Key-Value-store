// STUDENT_NAME: Bernard Bloch
// STUDENT_ID: 260632216
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <semaphore.h>
#include <sys/types.h>
#include <sys/shm.h>
#include "a2_lib.h"

// Number of buckets
#define POD_NUM 100
#define MAX_KEY_SIZE 32
#define MAX_VALUE_SIZE 256
#define MAX_PAIRS_PER_POD 100
#define TRUE 1
#define FALSE 0


// Defines a single key-value pair
struct pair
{
	char key[MAX_KEY_SIZE];
	char value[MAX_VALUE_SIZE];
	
};

// Defnes a single pod
struct pod
{
	struct pair pairs[MAX_PAIRS_PER_POD];
	int used;
	int cursor;
	char query[MAX_KEY_SIZE];
	sem_t mutex, db;
        int rc;

};

// shared memory file descriptor within virtual memory.
// Key-values is th entire hashmap
struct key_values
{
	struct pod pods[POD_NUM];
} *k_v = NULL;


char *createFilename = NULL; // global filename for mmap open file

struct pair_of_key_cursor
{
        char *key[MAX_KEY_SIZE];
        int cursor[];

};


// Creates shared memory within the physical and the virtual memory.
// assigns addr
// returns 0 is the shared memory was successfully created and otherwise -1
int kv_store_create(char *name)
{
	struct stat s;
	int fd;
	
	// check that the filename is not existing
	if(createFilename != NULL) {
		printf("kv_store_create: already opened %s.\n", createFilename);
		return -1;
	}
	
	fd = shm_open(name, O_CREAT|O_RDWR, S_IRWXU);

#if 0

	// new code -- crashes
	
	k_v = (struct key_values*)mmap(NULL, sizeof(struct key_values), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

	if(fd < 0) return -1;
	if(fstat(fd, &s) == -1) return -1;
	if(s.st_size == 0) {
		ftruncate(fd, sizeof(struct key_values));
		/*for(int pod = 0; pod < POD_NUM; pod++)
		{
			sem_init(&(k_v-> pods[pod].mutex) ,1,1);
			sem_init(&(k_v -> pods[pod].db), 1, 1);
			k_v -> pods[pod].rc = 0;
		}*/
	}
	//if(fstat(fd, &s) == -1) return -1;

#else
	
	// old code -- I guess it works?
	//k_v = (struct key_values*)mmap(NULL, s.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if(fd < 0) return -1;
	ftruncate(fd, sizeof(struct key_values));
	if(fstat(fd, &s) == -1) return -1;
	
	k_v = (struct key_values*)mmap(NULL, s.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
#endif
	
	if(k_v == NULL) return -1;
	
	printf("Created %s.\n", name);
	createFilename = name;

	
	return 0;
}

// hashing function bounded by pod number
int calculate_key(char *key)
{
	int hashAddress = 5381;
	for (int counter = 0; key[counter]!='\0'; counter++)
	{
        	hashAddress = ((hashAddress << 5) + hashAddress) + key[counter];
    	}
    	return hashAddress % POD_NUM < 0 ? -hashAddress % POD_NUM : hashAddress % POD_NUM;
}

// resets the read cursor
/*void reset_read_cursor(struct pod *pod) {
	if(pod->used < MAX_PAIRS_PER_POD) {
		pod -> readCursor = 0;
	} else {
		pod -> readCursor = (pod -> cursor + 1) % MAX_PAIRS_PER_POD;
	}
}*/

int kv_store_write(char *key, char *value)
{
	struct pod *pod;
	struct pair *pair;
	
	// the file must be open
	if(createFilename == NULL) {
		printf("Didn't call kv_store_create.\n");
		return -1;
	}
	// pick the calculate_key pod out of the pods array
	pod = k_v -> pods + calculate_key(key);
//	printf("pod number: %d\n", pod -> used);
	sem_wait(&pod -> db);
	// if the pod is full, evict the oldest one
	if(pod -> used == MAX_PAIRS_PER_POD) {
		for(int index = 0; index < MAX_PAIRS_PER_POD-1; index++)
			pod -> pairs[index] = pod -> pairs[index + 1];
//			memmove(pod -> pairs, pod -> pairs +  1, sizeof(struct pair) * (MAX_PAIRS_PER_POD));
		pod -> used--;
		if(pod -> cursor > 0) pod -> cursor--;
	}

	
	pair = pod -> pairs + pod -> used;
	pod -> used++;
	for(int p = 0; p < MAX_PAIRS_PER_POD; p++)
        {
                if(strcmp(value,pod->pairs[p].value ) == 0)
                {
			sem_post(&pod -> db);
                        return -1;
                }

        }

	// copies key and the value to the newest pair
	strncpy(pair -> key, key, MAX_KEY_SIZE);
	//pair -> key[MAX_KEY_SIZE - 1] = '\0';
	
	strncpy(pair -> value, value, MAX_VALUE_SIZE);
	//pair -> value[MAX_VALUE_SIZE - 1] = '\0';
	for(int i = strlen(key); i < MAX_KEY_SIZE; i++ )
		pair -> key[i] = '\0';
	
	for(int i = strlen(value); i < MAX_VALUE_SIZE; i++ )
                pair -> value[i] = '\0';

	/*{
		int i;
		for(i = 0; i < pod->used; i++) {
			printf("%d: %s -> %s\n", i, pod->pairs[i].key, pod->pairs[i].value);
		}
	}*/
	sem_post(&pod -> db);
	return 0;
}

void read_ent_prot(struct pod *pod)
{
        sem_wait(&pod -> mutex);
        pod -> rc = pod->rc + 1;
        if(pod -> rc == 1) sem_wait(&pod -> db);
        sem_post(&pod -> mutex);
}

void read_end_prot(struct pod *pod)
{
	sem_wait(&pod -> mutex);
        pod -> rc = pod -> rc - 1;
        if(pod -> rc == 0) sem_post(&pod -> db);
        	sem_post(&pod -> mutex);
}
/* "The kv_store_read() function takes a key and searches the store for the key-
 value pair. If found, it returns a copy of the value. It duplicates the string
 found in the store and returns a pointer to the string. It is the
 responsibility of the calling function to free the memory allocated for the
 string. If no key-value pair is found, a NULL value is returned." */
char *kv_store_read(char *key)
{
	char *str;
	struct pod *pod;
	struct pair *pair;
	int i, readEnd;
	int isLoop = TRUE;
	
	// the file must be open
	if(createFilename == NULL) {
		printf("Didn't call kv_store_create.\n");
		return NULL;
	}


	pod = k_v -> pods + calculate_key(key);
	//read_ent_prot(pod);
	// check whether the query is repeated. If it is not, reset it.
	// this deals with calling read repetedly
	if(strcmp(pod -> query, key) != 0) {
		strncpy(pod -> query, key, MAX_KEY_SIZE);
		for(int j = strlen(key); j < MAX_KEY_SIZE; j++)
		// make sure it's null-terminated
		pod -> query [j] = '\0';
	}
	
	// check the contents
	if(pod -> used == 0)
	{ 
		read_end_prot(pod);
		return NULL;
	}

	// total_read = pattern_length + 1
	// os_test1 ignores the last value? this doesn't help
	//readEnd = (pod -> cursor + pod -> used - 1) % pod -> used;

	//readEnd = pod -> cursor;
//	printf("Make enoug...%d\n", pod -> cursor);
	do {
		pair = pod -> pairs + pod -> cursor;
		//printf("%s -> %s\n", pair->key, pair->value);
		// increment readCursor

		pod -> cursor = (pod -> cursor + 1) % MAX_PAIRS_PER_POD;
		// if it is the one I'm looking for
		if(strcmp(pair -> key, key) == 0) {
			char *duplicate = strdup(pair -> value);
			//printf("returning %d, readCursor %d\n", i, pod->readCursor);
			//printf("Key: %s  Hash: %d  Value: %s\n", key, calculate_key(key), duplicate);
			read_end_prot(pod);
			return duplicate;
		}
	} while(pod -> cursor != 0);

	//pod -> cursor = 0;
	read_end_prot(pod);
	return NULL;
}

/* This function fulfills kv_store_read_all in read_eval.c.
 fixme: almost. for ~1/2 of the values, it os_test1 reads 1 less, seemingly
 randomly, but if discard one value, it reverses, but fails 100% in invalid
 ordering.
 
 eg
 
 Invalid Read Length Based on Pattern Analysis 86 != 85
 read_all_test: Invalid Ordering ... 85 255 171
 
 But clearly it's not. */
char **kv_store_read_all(char *key)
{
	char **strings;
	int str = 0;
	struct pod *pod;
	struct pair *pair;
	int i;
	
	// the file must be open
	if(createFilename == NULL) {
		printf("Didn't call kv_store_create.\n");
		return NULL;
	}
	pod = k_v -> pods + calculate_key(key);	
	read_ent_prot(pod);
	// allocate maximum space + null, strings, all allocated to null
	
	strings = calloc(pod -> used + 1, sizeof(char **));
	
	// fill the strings with matches
	for(i = 0; i < pod -> used; i++)
	{
		pair = pod -> pairs + i;
		if(strcmp(pair -> key, key) == 0) {
			char *duplicate = strdup(pair -> value);
			strings[str] = duplicate;
			str++;
		}
	}
	// null terminate
	strings[str] = '\0';
	// didn't find anything 
	if(str == 0) {
		free(strings);
		read_end_prot(pod);
		return NULL;
	}
	read_end_prot(pod);
	//free(strings);
//	printf("String is this: %s", *strings);
	return strings;
}

/* "You need to implement an extra function in the form
 
 int kv_delete_db()
 
 This is necessary because otherwise, the tester would have no way of unlinking the shared memory. 
 
 This should fully delete the shared memory database and all named semaphores."
 */
int kv_delete_db(void) {
	
	// the file must be open
	if(createFilename == NULL) {
		printf("Didn't call kv_store_create.\n");
		return -1;
	}
	/*for(int i = 0; i < POD_NUM; i++)
	{
		sem_destroy(&k_v -> pods[i].mutex);
		sem_destroy(&k_v -> pods[i].db);
	}*/
	if(munmap(k_v, sizeof *k_v)) {
		perror(createFilename);
	}
	if(shm_unlink(createFilename)) {
		perror(createFilename);
	} else {
		createFilename = NULL;
	}
	
	return 0;
}

// for debugging
void kv_store_print(void) {
	int p, q;
	struct pod *pod;
	struct pair *pair;
	
	// the file must be open
	if(createFilename == NULL) {
		printf("Didn't call kv_store_create.\n");
		return;
	}
	for(p = 0; p < POD_NUM; p++)
	{
		pod = k_v -> pods + p;
		printf("pod %d\n", p);
		for(q = 0; q < pod -> used; q++) {
			pair = pod -> pairs + q;
			printf("%.20s... -> %.30s... %s\n", pair -> key, pair -> value, q == pod -> cursor ? "cursor" : "no");
		}
		
	}
}

#if 0
int main()
{
	char *s;
	char **s_all;
	kv_store_create("myShared");
	kv_store_write("that", "works");
	s = kv_store_read("thattttttttttttttttttttttteeeeeery");
	printf("%s\n", s);
	kv_store_write("that", "should work");
	s = kv_store_read("that");
	printf("kv_store_read(that): %s\n", s);
	s_all = kv_store_read_all("that");
	printf("%s\n", s);
	printf("%s\n", s_all[0]);
	printf("%s\n", s_all[1]);
	free(s);
	free(s_all);
	unlink("myShared");
	return 0;
}
#endif
