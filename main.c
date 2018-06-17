#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// NOTE: sizeof(int) is 4 bytes (but we shouldn't depend on this)

// TODO: These should come from the user input
// const int PAGE_SIZE = 40;   // in bytes
// const int NUM_PAGES = 10;

// const int HASH_MOD = 5;


// Structure that defines a row in the input table
typedef struct TableRow {
    int fieldId;
    int fieldValue;
} TableRow;

int getHashValue(int value) {
    int p = HASH_MOD;
    return value % p;
}

void writePageToFile(char* filename, TableRow memoryPage[], int pageSize) {
    FILE *f = fopen(filename, "w");
    for (int i=0; i<pageSize; i++) {
        fprintf(f, "%d %d\n", memoryPage[i].fieldId, memoryPage[i].fieldValue);
    }
    fclose(f);
}

int main(int argc, char *argv[]) {

    // input parameters
    int pageSizeBytes, numPagesInBuffer, hashMod;

    // Get user input
    printf("We are going to produce a hash-join.\nFirst we need some information..\n\n");
    printf("What is the system's page size (in bytes) ?");
    scanf("%d", &pageSizeBytes);
    printf("\nHow many pages are in each buffer ?");
    scanf("%d", &numPagesInBuffer);
    printf("\nHow many buckets should we hash ?");
    scanf("%d", &hashMod);

    // Say hello, and print command line arguments
    // printf("hello world\n");
    // for (int i=0; i<argc; i++) {
        // printf("arg %d: %s\n", i, argv[i]);
    // }
    
    // Calculate how many rows fit in one page
    int numRowsInPage = pageSize / (sizeof(int) + sizeof(int));
    printf("We can fit %d table rows in each memory page.\n", numRowsInPage);

    // Create memory page structure
    TableRow memoryPage[numRowsInPage];

    // read datafile
    int i=0;
    char *filename = "D:\\OpenU\\DB Implementation\\datafile.txt";
    FILE *fp = fopen(filename, "r"); // open in read only mode
    char fileLine[256];
    while (fgets(fileLine, sizeof(fileLine), fp)) {
        // If line is longer than 256 characters, we need to read more.
        printf("%s", fileLine);
        int fieldSize = 5;

        char* field = malloc(fieldSize * sizeof(char));
        strncpy(field, fileLine, fieldSize);
        char* value = malloc(fieldSize * sizeof(char));
        strncpy(value, fileLine + fieldSize + 1, 5);

        if (i < numRowsInPage) {
            memoryPage[i].fieldId = atoi(field);
            memoryPage[i].fieldValue = atoi(value);
        }

        i++;
    }

    writePageToFile("gilly.txt", memoryPage, numRowsInPage);

    fclose(fp);
    return 0;
}
