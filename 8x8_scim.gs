function getUserScim() {
  var token = '<YOUR>-API-Token';
  var url = 'https://platform-cloud.8x8.com/udi/customers/<YOUR>-Customer-ID/scim/v2';
  var urlParams = url + '/Users?startIndex=1&count=100';

  var params = {
    'headers': {  // Changed Headers to headers (lowercase)
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    },
    'method': 'get'  // Added method explicitly
  };

  try {
    var scimUsers = UrlFetchApp.fetch(urlParams, params);
    var scimData = JSON.parse(scimUsers.getContentText());  // Fixed JSON.Parse to JSON.parse and used getContentText()

    Logger.log(scimData);
    return scimData;  // Added return for better function usage
  } catch (e) {
    Logger.log('Error fetching SCIM users: ' + e);
    return null;
  }
}
