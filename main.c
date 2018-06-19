#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// TODO: There's a bug with the zero value! (delete it, or make it work!)

const char* DATA_DIR = "./data/";
const char* RESULTS_DIR = "./results/";
const char* JOIN_FILE = "./results/join";

const int FIELD_SIZE = 5;

// Structure that defines a row in the input table
typedef struct TableRow {
    int fieldId;
    int fieldValue;
} TableRow;

/**
 * This converts a string to a TableRow object
 * When reading the table file, we pass each line to this method,
 * and get a TableRow object in return (as a passed pointer parameter)
 */
void deserializeRow(char* rowString, TableRow* rowObject) {

    char* field = malloc(FIELD_SIZE * sizeof(char));
    char* value = malloc(FIELD_SIZE * sizeof(char));

    // Structure of a table row in the file system is:
    // <field1><single space><field2>\n
    strncpy(field, rowString, FIELD_SIZE);
    strncpy(value, rowString + 1 + FIELD_SIZE, FIELD_SIZE);

    rowObject->fieldId = atoi(field);
    rowObject->fieldValue = atoi(value);

    free(field);
    free(value);
}

/**
 * Takes an array of TableRow objects and writes them
 * to a specific filename.
 */
void writePageToFile(char* filename, TableRow* memoryPage, int pageSize) {
    FILE* pageFile = fopen(filename, "a");
    for (int i=0; i < 3; i++) {
        if (memoryPage[i].fieldId == 0 && memoryPage[i].fieldValue == 0)
            continue;
        fprintf(pageFile, "%d %d\n", memoryPage[i].fieldId, memoryPage[i].fieldValue);
    }
    fclose(pageFile);
}

/**
 * Count how many digits a specific number (int) has
 * This is useful for allocating memory for strings we'll
 * need for creating filenames.
 */
int numDigits(int num) {
    int digitCount = 0;
    while (num / 10 > 0)
        digitCount++;
    return digitCount;
}

/**
 * Saves a bucket to the disk, and clears the bucket array
 * so we have room to read more.
 */
void saveAndClearBucket(const char* tableName, int hashValue, TableRow** buckets, int numRowsInPage) {

    char* bucketFileName = malloc(sizeof(tableName) + (sizeof(char) * numDigits(hashValue)) + sizeof(RESULTS_DIR) + 1);

    strcpy(bucketFileName, RESULTS_DIR);
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
    free(bucketFileName);
}

/**
 * Goes over a specific table, and splits the table into
 * hash buckets.
 * A single file for each hash bucket.
 * This method needs to receive the name of the table,
 * the amount of buckets and the number of rows in each memory page.
 */
void splitTableToBuckets(const char* tableName, int hashMod, int numRowsInPage) {

    TableRow **hashBuckets = (TableRow **)malloc(hashMod * sizeof(TableRow *));
    for (int i = 0; i < hashMod; i++)
        hashBuckets[i] = (TableRow *)malloc(numRowsInPage * sizeof(TableRow));

    int hashBucketsCount[hashMod];
    for (int i=0; i<hashMod; i++) 
        hashBucketsCount[i] = 0;

    // construct the table filename
    char* tableFilename = malloc(sizeof(tableName) + sizeof(DATA_DIR));
    strcpy(tableFilename, DATA_DIR);
    strcat(tableFilename, tableName);

    int lineSize = sizeof(char) * (FIELD_SIZE + FIELD_SIZE + 3);
    char fileLine[lineSize];        // We can assume that each line in the file is smaller than a memory page

    // Start reading the table data file
    FILE *fp = fopen(tableFilename, "r");
    while (fgets(fileLine, sizeof(fileLine), fp)) {

        TableRow tableRow;
        deserializeRow(fileLine, &tableRow);

        // place the row in the designated bucket
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

    // free memory
    fclose(fp);
    for (int i=0; i<hashMod; i++)
        free(hashBuckets[i]);
    free(hashBuckets);
    free(tableFilename);
}

/**
 * Read a whole bucket file into memory.
 * This finds the specific bucket file according to a given
 * table name and bucket number, and returns an array of 
 * TableRow objects with all the rows in this bucket.
 * The array returned must be 'freed' not to cause a memory leak.
 */
TableRow* readBucketFileToMemory(const char* tableName, int bucketNum, int numRowsInPage, int* bucketCount) {

    // construct the name of the bucket file
    char* bucketFilename = malloc(sizeof(tableName) + sizeof(RESULTS_DIR) + 2);
    strcpy(bucketFilename, RESULTS_DIR);
    strcat(bucketFilename, tableName);
    char* t = malloc(sizeof(char) * numDigits(bucketNum));
    sprintf(t, "_%d", bucketNum);
    strcat(bucketFilename, t);
    free(t);

    // TODO: make sure this file exists!!
    FILE* bucketFile = fopen(bucketFilename, "r");      // TODO: We assume all bucket files exist. We should probably make sure the file exists!
    TableRow* bucketRows = malloc(sizeof(TableRow) * numRowsInPage);

    int lineSize = sizeof(char) * (FIELD_SIZE + FIELD_SIZE + 3);
    char fileLine[lineSize];        // We can assume that each line in the file is smaller than a memory page

    int count = 0;
    while (fgets(fileLine, sizeof(fileLine), bucketFile)) {
        
        TableRow tableRow;
        deserializeRow(fileLine, &tableRow);

        bucketRows[count] = tableRow;
        
        if (++count % numRowsInPage == 0) {
            bucketRows = realloc(bucketRows, sizeof(bucketRows) + (sizeof(TableRow) * numRowsInPage));
        }
    }
    fclose(bucketFile);
    free(bucketFilename);

    *bucketCount = count;
    return bucketRows;
}

/**
 * Join the two tables
 * by reading the hash table bucket files, and joining
 * them to a final file with the 'joined result'
 */
void joinTables(int numBuckets, int numRowsInPage, const char* firstTable, const char* secondTable) {

    // read each bucket file into memory
    FILE* joinFile = fopen(JOIN_FILE, "w");

    for (int n=0; n<numBuckets; n++) {

        int bucketCountOne, bucketCountTwo;
        TableRow* bucketFileOne = readBucketFileToMemory(firstTable, n, numRowsInPage, &bucketCountOne);
        TableRow* bucketFileTwo = readBucketFileToMemory(secondTable, n, numRowsInPage, &bucketCountTwo);

        // Iterate over the first bucket file, and join the values with the second
        for (int i=0; i<bucketCountOne; i++) {
            for (int j=0; j<bucketCountTwo; j++) {
                if (bucketFileTwo[j].fieldId == bucketFileOne[i].fieldId) {
                    fprintf(joinFile, "%d %d %d\n", bucketFileOne[i].fieldId, bucketFileOne[i].fieldValue, bucketFileTwo[j].fieldValue);
                }
            }
        }
        // TODO: we might have rows here that we didn't put in the file!!
    }
    fclose(joinFile);
}

/**
 * Collects the user input so we can start joining :)
 */
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
    getUserInput(&pageSizeBytes, &numPagesInBuffer, &hashMod);

    // Calculate how many rows fit in one page
    int numRowsInPage = pageSizeBytes / sizeof(TableRow);

    // TODO: these should come from the user
    const char* table1 = "table1";
    const char* table2 = "table2";

    splitTableToBuckets(table1, hashMod, numRowsInPage);
    splitTableToBuckets(table2, hashMod, numRowsInPage);

    joinTables(hashMod, numRowsInPage, table1, table2);

    // TODO: output some statistics at the end

    return 0;
}
