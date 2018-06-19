#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// NOTE: sizeof(int) is 4 bytes (but we shouldn't depend on this)

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
    for (int i=0; i < 3; i++) {
        if (memoryPage[i].fieldId == 0 && memoryPage[i].fieldValue == 0) {
            continue;
        }
        fprintf(pageFile, "%d %d\n", memoryPage[i].fieldId, memoryPage[i].fieldValue);
    }
    c++;
    fclose(pageFile);
}

int numDigits(int num) {
    int digitCount = 0;
    while (num / 10 > 0) {
        digitCount++;
    }
    return digitCount;
}

void saveAndClearBucket(const char* tableName, int hashValue, TableRow** buckets, int numRowsInPage) {

    char* bucketFileName = malloc(sizeof(tableName) + (sizeof(char) * numDigits(hashValue)) + sizeof(dir) + 1);

    strcpy(bucketFileName, dir);
    strcat(bucketFileName, tableName);
    char hashValueStr[numDigits(hashValue)];
    sprintf(hashValueStr, "_%d", hashValue);
    strcat(bucketFileName, hashValueStr);

    writePageToFile(bucketFileName, buckets[hashValue], numRowsInPage);

    // clear bucket for further use
    for (int i=0; i<numRowsInPage; i++) {
        buckets[hashValue][i].fieldId = 0;
        buckets[hashValue][i].fieldValue = 0;
    }
}

void splitTableToBuckets(const char* tableName, int hashMod, int numRowsInPage) {

    TableRow **hashBuckets = (TableRow **)malloc(hashMod * sizeof(TableRow *));
    for (int i = 0; i < hashMod; i++)
        hashBuckets[i] = (TableRow *)malloc(numRowsInPage * sizeof(TableRow));

    int hashBucketsCount[hashMod];
    for (int i=0; i<hashMod; i++) 
        hashBucketsCount[i] = 0;

    // start reading table from data directory
    char* tableDir = "./data/";
    char* tableFilename = malloc(sizeof(tableName) + sizeof(tableDir));
    strcpy(tableFilename, tableDir);
    strcat(tableFilename, tableName);

    FILE *fp = fopen(tableFilename, "r");    // open in read only mode

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
        hashBuckets[hashValue][bucketCounter].fieldId = atoi(field);
        hashBuckets[hashValue][bucketCounter].fieldValue = atoi(value);

        hashBucketsCount[hashValue] = hashBucketsCount[hashValue] + 1;
        if (hashBucketsCount[hashValue] == numRowsInPage) {
            saveAndClearBucket(tableName, hashValue, hashBuckets, numRowsInPage);
            hashBucketsCount[hashValue] = 0;
        }

        free(field);
        free(value);
    }

    // Save the hash buckets that are left
    for (int i=0; i<hashMod; i++) {
        saveAndClearBucket(tableName, i, hashBuckets, numRowsInPage);
    }

    fclose(fp);
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
    printf("We can fit %d table rows in each memory page.\n", numRowsInPage);

    const char* table1 = "table1";
    const char* table2 = "table2";

    splitTableToBuckets(table1, hashMod, numRowsInPage);
    splitTableToBuckets(table2, hashMod, numRowsInPage);

    printf("\n\nall done :)");
    printf("\nwrote %d pages", c);

    // writePageToFile("gilly.txt", memoryPage, numRowsInPage);

    return 0;
}
