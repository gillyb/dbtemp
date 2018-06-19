#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// NOTE: sizeof(int) is 4 bytes (but we shouldn't depend on this)

// TODO: These should come from the user input
// const int PAGE_SIZE = 40;   // in bytes
// const int NUM_PAGES = 10;
// const int HASH_MOD = 5;

const char* dir = "./results/";
int c = 0;

// Structure that defines a row in the input table
typedef struct TableRow {
    int fieldId;
    int fieldValue;
} TableRow;

void writePageToFile(char* filename, TableRow* memoryPage, int pageSize) {
    FILE* pageFile = fopen(filename, "a");
    // int memoryPageSize = sizeof(memoryPage) / sizeof(TableRow);
    fprintf(pageFile, "\n");
    for (int i=0; i < 3; i++) {
        if (memoryPage[i].fieldId == 0 && memoryPage[i].fieldValue == 0) {
            fprintf(pageFile, "...\n");
            continue;
        }
        // printf(".");
        fprintf(pageFile, "%d %d\n", memoryPage[i].fieldId, memoryPage[i].fieldValue);
    }
    c++;
    fclose(pageFile);
}

int c2 = 0;
void saveAndClearBucket(int hashValue, TableRow** buckets, int numRowsInPage) {
    char hashFilename[100];
    char hashValueStr[5];

    sprintf(hashValueStr, "%d", hashValue);
    strcpy(hashFilename, dir);
    strcat(hashFilename, hashValueStr);

    printf("saving %d rows to hash file %d", numRowsInPage, hashValue);
    c2 += numRowsInPage;

    writePageToFile(hashFilename, buckets[hashValue], numRowsInPage);

    // memset(bucket, '\0', sizeof(bucket));
    for (int i=0; i<numRowsInPage; i++) {
        buckets[hashValue][i].fieldId = 0;
        buckets[hashValue][i].fieldValue = 0;
    }
}

void getUserInput(int* pageSizeBytes, int* numPagesInBuffer, int* hashMod) {
    // TODO: Check the limits of these (according to page 137)
    printf("We are going to produce a hash-join.\nFirst we need some information..\n\n");
    printf("What is the system's page size (in bytes) ?");
    scanf("%d", pageSizeBytes);
    printf("\nHow many pages are in each buffer ?");
    scanf("%d", numPagesInBuffer);
    printf("\nHow many buckets should we hash ?");
    scanf("%d", hashMod);

    printf("\npageSizeBytes: %d\n", *pageSizeBytes);
    printf("numPagesInBuffer: %d\n", *numPagesInBuffer);
    printf("hashMod: %d\n\n", *hashMod);
}

int main(int argc, char *argv[]) {

    // input parameters
    int pageSizeBytes, numPagesInBuffer, hashMod;
    // getUserInput(&pageSizeBytes, &numPagesInBuffer, &hashMod);
    pageSizeBytes = 24;
    numPagesInBuffer = 12;
    hashMod = 4;

    // Calculate how many rows fit in one page
    int numRowsInPage = pageSizeBytes / sizeof(TableRow);
    // printf("\nEach row takes %d bytes\n", sizeof(TableRow));
    printf("We can fit %d table rows in each memory page.\n", numRowsInPage);





    // Create hash buckets
    // We create 'hashMod' buckets (This is the amount of buckets from the user input)
    // Each bucket has 'numRowsInPage' table rows (This is the amount of rows that can fit in 1 memory page)
    // TableRow hashBuckets[hashMod][numRowsInPage];
    // TableRow hashBuckets[hashMod][4];

    TableRow **hashBuckets = (TableRow **)malloc(hashMod * sizeof(TableRow *));
    for (int i = 0; i < hashMod; i++)
        hashBuckets[i] = (TableRow *)malloc(numRowsInPage * sizeof(TableRow));

    int hashBucketsCount[hashMod];
    for (int i=0; i<hashMod; i++) hashBucketsCount[i] = 0;
    // memset(hashBucketsCount, 0, hashMod);

    // start reading table from datafile
    int i=0;
    char *filename = "./datafile.txt";  // TODO: get this from the user
    FILE *fp = fopen(filename, "r");    // open in read only mode

    int fieldSize = 5;
    int lineSize = sizeof(char) * (fieldSize + fieldSize + 3);

    char fileLine[lineSize];        // We can assume that each line in the file is smaller than a memory page
    while (fgets(fileLine, sizeof(fileLine), fp)) {

        int fieldSize = 5;

        char* field = malloc(fieldSize * sizeof(char));
        char* value = malloc(fieldSize * sizeof(char));

        strncpy(field, fileLine, fieldSize);
        strncpy(value, fileLine + 1 + fieldSize, fieldSize);


        int hashValue = atoi(field) % hashMod;      // Get the hash for this id
        // printf(":%d", hashValue);

        int bucketCounter = hashBucketsCount[hashValue];
        // printf("  hashValue: %d\n", hashValue);
        hashBuckets[hashValue][bucketCounter].fieldId = atoi(field);
        hashBuckets[hashValue][bucketCounter].fieldValue = atoi(value);

        // printf("  ->: %d\n", hashValue);

        hashBucketsCount[hashValue] = hashBucketsCount[hashValue] + 1;
        if (hashBucketsCount[hashValue] == numRowsInPage) {
            saveAndClearBucket(hashValue, hashBuckets, numRowsInPage);
            hashBucketsCount[hashValue] = 0;
        }

        // free(field);
        // free(value);
        printf("i: %d, ", i);
        i++;
    }

    printf("\ntotal count: %d", i);
    // Save the hash buckets that are left
    for (int i=0; i<hashMod; i++) {
        saveAndClearBucket(i, hashBuckets, numRowsInPage);
    }

    // test
    for (int i=0; i<hashMod; i++) {
        printf("\n\nbucket %d: %d rows\n", i, hashBucketsCount[i]);
    }

    printf("\n\nall done :)");
    printf("\nwrote %d pages", c);
    printf("\nagain, wrote %d rows", c2);

    // writePageToFile("gilly.txt", memoryPage, numRowsInPage);

    fclose(fp);
    return 0;
}
