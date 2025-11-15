/**
 * Google Apps Script for fetching 8x8 User Details via the SCIM API
 * and loading them into a Google BigQuery table.
 *
 * This script is based on the successful CDR pipeline and has been corrected
 * to use the SCIM API endpoint and Bearer token authentication as advised by 8x8 support.
 * It implements a staging and merge pattern to prevent duplicate records.
 */

// --- Configuration ---
// CORRECTED (v5): Using the correct SCIM API endpoint and authentication token.
var USERS_API_ENDPOINT_BASE = 'https://platform-cloud.8x8.com/udi/customers/';
var SCIM_API_TOKEN = '<YOUR>-API-Token'; // Static Bearer token for SCIM API

var BIGQUERY_PROJECT_ID = '<YOUR>-migration-project-8x8';
var BIGQUERY_DATASET_ID = '<YOUR>-8x8DataSet';
var BIGQUERY_TABLE_ID = '<YOUR>-8x8Users'; // Main table for user data
var BIGQUERY_STAGING_TABLE_ID = '<YOUR>-8x8Users_staging'; // Staging table for user data

var API_PAGE_SIZE = 100; // The 'count' parameter for the SCIM API

// --- Main Orchestration Function ---
function sync8x8UsersToBigQuery() {
  Logger.log('sync8x8UsersToBigQuery CALLED - START');

  var scriptProperties = PropertiesService.getScriptProperties();
  var customerId = scriptProperties.getProperty('customer_id');

  if (!customerId) {
    var errorMessage = "sync8x8UsersToBigQuery: ERROR - Missing 'customer_id' from Script Properties.";
    Logger.log(errorMessage);
    throw new Error(errorMessage);
  }
  Logger.log('sync8x8UsersToBigQuery - STEP 1: Customer ID found in Script Properties.');

  try {
    Logger.log('sync8x8UsersToBigQuery - STEP 2: Fetching all users from SCIM API...');
    var allUsers = fetchAllUsers(SCIM_API_TOKEN, customerId);
    
    Logger.log('sync8x8UsersToBigQuery - STEP 3: Total raw user records fetched: %s', allUsers.length);

    if (allUsers.length > 0) {
      var bqRows = allUsers.map(transformUserRecordToBigQueryRow).filter(function(row) { return row !== null; });
      Logger.log('sync8x8UsersToBigQuery - STEP 4: Transformed %s user records for BigQuery.', bqRows.length);

      if (bqRows.length > 0) {
        loadAndMergeDataInBigQuery(bqRows);
        Logger.log('sync8x8UsersToBigQuery - STEP 5: Staging and merging process initiated for user data.');
      }
    }

    Logger.log('sync8x8UsersToBigQuery FINISHED successfully.');

  } catch (e) {
    Logger.log('sync8x8UsersToBigQuery: ERROR during data processing or BigQuery load: %s \n Stack: %s', e.toString(), e.stack);
    throw new Error('Error during 8x8 user data sync: ' + e.message);
  }
}

// --- 8x8 Data Fetching ---
/**
 * REWRITTEN (v5): Fetches all users from the SCIM API endpoint using a static Bearer token.
 */
function fetchAllUsers(token, customerId) {
  Logger.log('fetchAllUsers CALLED - START.');
  var allUsers = [];
  var startIndex = 1;
  var totalResults = 0;

  var baseUrl = USERS_API_ENDPOINT_BASE + customerId + '/scim/v2/Users';

  var options = {
    'method': 'get',
    'headers': {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    },
    'muteHttpExceptions': true
  };

  var pageCount = 0;
  var MAX_USER_PAGES_TO_FETCH = 100; // Safety break

  do {
    pageCount++;
    var currentUrl = baseUrl + '?startIndex=' + startIndex + '&count=' + API_PAGE_SIZE;
    Logger.log('fetchAllUsers: Fetching user list (API call #%s): %s', pageCount, currentUrl);
    
    var response = UrlFetchApp.fetch(currentUrl, options);
    var responseCode = response.getResponseCode();
    var responseBody = response.getContentText();

    if (responseCode !== 200) {
      throw new Error('API Error fetching user list: ' + responseCode + ' - ' + responseBody);
    }
    
    var jsonResponse = JSON.parse(responseBody);
    var userListOnPage = jsonResponse.Resources || [];
    totalResults = jsonResponse.totalResults || 0;
    
    Logger.log('fetchAllUsers: Received %s user records on API call #%s. Total results reported by API: %s', userListOnPage.length, pageCount, totalResults);
    
    if (userListOnPage.length > 0) {
      allUsers = allUsers.concat(userListOnPage);
    }
    
    startIndex += userListOnPage.length; // Increment start index for the next page
    
    Utilities.sleep(500); // Be polite to the API

  } while (allUsers.length < totalResults && pageCount < MAX_USER_PAGES_TO_FETCH);

  Logger.log('fetchAllUsers: Total users collected: %s.', allUsers.length);
  return allUsers;
}


// --- Data Transformation ---
/**
 * REWRITTEN (v3): Transforms a raw user record from the SCIM API into a row for BigQuery.
 */
function transformUserRecordToBigQueryRow(apiRecord) {
  try {
    // SCIM API returns data in a nested structure. We need to flatten it.
    var row = {
      id: apiRecord.id || null,
      userName: apiRecord.userName || null,
      givenName: (apiRecord.name && apiRecord.name.givenName) ? apiRecord.name.givenName : null,
      familyName: (apiRecord.name && apiRecord.name.familyName) ? apiRecord.name.familyName : null,
      email: (Array.isArray(apiRecord.emails) && apiRecord.emails.length > 0) ? apiRecord.emails[0].value : null,
      active: apiRecord.active || false,
      created: (apiRecord.meta && apiRecord.meta.created) ? apiRecord.meta.created : null,
      lastModified: (apiRecord.meta && apiRecord.meta.lastModified) ? apiRecord.meta.lastModified : null
    };
    
    return row;

  } catch (e) {
    Logger.log('CRITICAL ERROR in transformUserRecordToBigQueryRow for record: ' + JSON.stringify(apiRecord) + '. Error: ' + e.toString());
    return null;
  }
}


// --- BigQuery Loading ---
/**
 * Orchestrates loading data to a staging table and then merging into the main table.
 */
function loadAndMergeDataInBigQuery(rows) {
  if (!rows || rows.length === 0) {
    Logger.log('loadAndMergeDataInBigQuery: No rows to load.');
    return;
  }
  
  Logger.log('loadAndMergeDataInBigQuery: Preparing to load %s rows into STAGING table: %s',
    rows.length, BIGQUERY_STAGING_TABLE_ID);

  var dataAsJson = rows.map(function(row) { return JSON.stringify(row); }).join('\n');
  var dataAsBlob = Utilities.newBlob(dataAsJson, 'application/json');
  
  // UPDATED (v3): Define the schema to match the new SCIM-based table.
  var bqSchema = {
    fields: [
      {name: 'id', type: 'STRING'},
      {name: 'userName', type: 'STRING'},
      {name: 'givenName', type: 'STRING'},
      {name: 'familyName', type: 'STRING'},
      {name: 'email', type: 'STRING'},
      {name: 'active', type: 'BOOLEAN'},
      {name: 'created', type: 'TIMESTAMP'},
      {name: 'lastModified', type: 'TIMESTAMP'}
    ]
  };

  var stagingJob = {
    configuration: {
      load: {
        destinationTable: {
          projectId: BIGQUERY_PROJECT_ID,
          datasetId: BIGQUERY_DATASET_ID,
          tableId: BIGQUERY_STAGING_TABLE_ID
        },
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        writeDisposition: 'WRITE_TRUNCATE',
        schema: bqSchema,
        autodetect: false
      }
    }
  };

  try {
    var loadJob = BigQuery.Jobs.insert(stagingJob, BIGQUERY_PROJECT_ID, dataAsBlob);
    Logger.log('Load to staging table started. Job ID: %s', loadJob.jobReference.jobId);
    waitForJob(loadJob.jobReference.jobId);
  } catch (e) {
    Logger.log('Error inserting BigQuery STAGING load job: ' + e.toString());
    throw new Error('BigQuery staging load job insertion failed: ' + e.message);
  }
  
  Logger.log('loadAndMergeDataInBigQuery: Staging table loaded. Now running MERGE query.');
  runBigQueryMerge();
}

function waitForJob(jobId) {
  var sleepTimeMs = 500;
  while (true) {
    var job = BigQuery.Jobs.get(BIGQUERY_PROJECT_ID, jobId);
    if (job.status.state === 'DONE') {
      if (job.status.errorResult) {
        throw new Error('BQ job failed: ' + JSON.stringify(job.status.errors));
      }
      Logger.log('Job %s completed successfully.', jobId);
      return;
    }
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
  }
}

/**
 * UPDATED (v3): Runs the MERGE query for the new SCIM-based user table.
 */
function runBigQueryMerge() {
  var mergeSql = `
    MERGE \`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_TABLE_ID}\` T
    USING \`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_STAGING_TABLE_ID}\` S
    ON T.id = S.id
    WHEN NOT MATCHED THEN
      INSERT(id, userName, givenName, familyName, email, active, created, lastModified)
      VALUES(S.id, S.userName, S.givenName, S.familyName, S.email, S.active, S.created, S.lastModified)
    WHEN MATCHED THEN
      UPDATE SET
        T.userName = S.userName,
        T.givenName = S.givenName,
        T.familyName = S.familyName,
        T.email = S.email,
        T.active = S.active,
        T.lastModified = S.lastModified
  `;

  var mergeJob = {
    configuration: {
      query: {
        query: mergeSql,
        useLegacySql: false
      }
    }
  };

  try {
    var queryJob = BigQuery.Jobs.insert(mergeJob, BIGQUERY_PROJECT_ID);
    Logger.log('MERGE job started. Job ID: %s', queryJob.jobReference.jobId);
    waitForJob(queryJob.jobReference.jobId);
  } catch (e) {
    Logger.log('Error running MERGE job: ' + e.toString());
    throw new Error('BigQuery MERGE job failed: ' + e.message);
  }
}


// --- Utility / Test Functions ---
/**
 * UPDATED (v5): Simplified to reflect the new authentication method.
 */
function manuallyFetchAndLogSampleUsers() {
  Logger.log('manuallyFetchAndLogSampleUsers CALLED');
  
  // This test function now directly calls the main sync function,
  // as the authentication is hardcoded and doesn't need a separate test.
  // This ensures the test is identical to a real run.
  sync8x8UsersToBigQuery();
}
