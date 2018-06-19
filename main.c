#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// NOTE: sizeof(int) is 4 bytes (but we shouldn't depend on this)

// TODO: There's a bug with the zero value! (delete it, or make it work!)

const char* dir = "./results/";
int c = 0;

// Structure that defines a row in the input table
typedef struct TableRow {
    int fieldId;
    int fieldValue;
} TableRow;

/**
 * This converts a string to a TableRow object
 */
void deserializeRow(char* rowString, TableRow* rowObject) {
    const int fieldSize = 5;        // Each field in our table is 5 bytes

    char* field = malloc(fieldSize * sizeof(char));
    char* value = malloc(fieldSize * sizeof(char));

    strncpy(field, rowString, fieldSize);
    strncpy(value, rowString + 1 + fieldSize, fieldSize);

    rowObject->fieldId = atoi(field);
    rowObject->fieldValue = atoi(value);

    free(field);
    free(value);
}

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

        TableRow tableRow;
        deserializeRow(fileLine, &tableRow);

        // place in bucket
        int hashValue = tableRow.fieldId % hashMod;
        int bucketCounter = hashBucketsCount[hashValue];
        hashBuckets[hashValue][bucketCounter] = tableRow;

        hashBucketsCount[hashValue] = hashBucketsCount[hashValue] + 1;
        if (hashBucketsCount[hashValue] == numRowsInPage) {
            saveAndClearBucket(tableName, hashValue, hashBuckets, numRowsInPage);
            hashBucketsCount[hashValue] = 0;
        }
    }

    // Save the hash buckets that are left
    for (int i=0; i<hashMod; i++) {
        saveAndClearBucket(tableName, i, hashBuckets, numRowsInPage);
    }

    fclose(fp);
}

TableRow* readBucketFileToMemory(const char* tableName, int bucketNum, int numRowsInPage, int* bucketCount) {

    // construct the name of the bucket file
    char* bucketFilename = malloc(sizeof(tableName) + sizeof(dir) + 2);
    strcpy(bucketFilename, dir);
    strcat(bucketFilename, tableName);
    char* t = malloc(sizeof(char) * numDigits(bucketNum));
    sprintf(t, "_%d", bucketNum);
    strcat(bucketFilename, t);
    free(t);

    // TODO: make sure this file exists!!
    FILE* bucketFile = fopen(bucketFilename, "r");      // TODO: We assume all bucket files exist. We should probably make sure the file exists!
    TableRow* bucketRows = malloc(sizeof(TableRow) * numRowsInPage);

    int fieldSize = 5;
    int lineSize = sizeof(char) * (fieldSize + fieldSize + 3);
    char fileLine[lineSize];        // We can assume that each line in the file is smaller than a memory page

    int count = 0;
    // printf("about to read %s", bucketFileName);
    while (fgets(fileLine, sizeof(fileLine), bucketFile)) {
        
        TableRow tableRow;
        deserializeRow(fileLine, &tableRow);

        bucketRows[count] = tableRow;
        
        count++;
        if (count % numRowsInPage == 0) {
            printf("reallocating\n");   // TEST
            bucketRows = realloc(bucketRows, sizeof(bucketRows) + (sizeof(TableRow) * numRowsInPage));
        }
    }
    printf("count: %d, size: %d", count, sizeof(TableRow));
    fclose(bucketFile);

    *bucketCount = count;
    return bucketRows;
}

// We assume we're joining between two tables only
void joinTables(int numBuckets, int numRowsInPage, const char* firstTable, const char* secondTable) {

    int bucketNum = 0;
    char* joinFilename = "./results/join";
    FILE* joinFile = fopen(joinFilename, "w");

    // read each bucket file into memory
    for (int n=0; n<numBuckets; n++) {

        int bucketCountOne, bucketCountTwo;
        TableRow* bucketFileOne = readBucketFileToMemory(firstTable, n, numRowsInPage, &bucketCountOne);
        TableRow* bucketFileTwo = readBucketFileToMemory(secondTable, n, numRowsInPage, &bucketCountTwo);

        // Iterate over the first bucket file, and join them
        for (int i=0; i<bucketCountOne; i++) {
            for (int j=0; j<bucketCountTwo; j++) {
                if (bucketFileTwo[j].fieldId == bucketFileOne[i].fieldId)
                    fprintf(joinFile, "%d %d %d\n", bucketFileOne[i].fieldId, bucketFileOne[i].fieldValue, bucketFileTwo[j].fieldValue);
            }
        }
        // TODO: we might have rows here that we didn't put in the file!!

        // TEST: print out bucketRows
        printf("\n\n We finished reading the whole bucket file. printing : ");
        for (int i=0; i<bucketCountOne; i++) {
            printf("id: %d, value: %d\n", bucketFileOne[i].fieldId, bucketFileOne[i].fieldValue);
        }

        free(bucketFileOne);
        free(bucketFileTwo);
    }

    // iterate over one of them and for each row
    //      find the matching row in the other bucket
    //      write them to a new 'joined' file

    fclose(joinFile);
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

    // TODO: these should come from the user
    const char* table1 = "table1";
    const char* table2 = "table2";

    splitTableToBuckets(table1, hashMod, numRowsInPage);
    splitTableToBuckets(table2, hashMod, numRowsInPage);

    joinTables(hashMod, numRowsInPage, table1, table2);

    // TODO: output some statistics at the end

    return 0;
}
