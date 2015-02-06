//Set options in function scope in the end.
(function(opts){
    // Make sure we have a base object for opts
    opts = opts || {};
    // Setup defaults for options
    opts.url = opts.url || null;
    opts.vars = opts.vars || {};
    opts.vars.cb = Math.floor(Math.random() * 10e12);
    opts.vars.visitorId = opts.visitorId;
    opts.vars.domain = document.location.hostname;
    opts.vars.page = document.location.pathname;
    opts.error = opts.error || function(){};
    opts.success = opts.success || function(){};
    
    // Split up vars object into an array
    var varsArray = [];
for(var key in opts.vars){ 
	    if(opts.vars[key] !== undefined){
		varsArray.push(encodeURIComponent(key)+'='+encodeURIComponent(opts.vars[key])); 
	    }
	}
    // Build query string
    var qString = varsArray.join('&');
 
    // Create a beacon if a url is provided
    if( opts.url )
    {
        // Create a brand NEW image object
        //var beacon = new Image();
        var beacon = document.createElement("img");
        // Attach the event handlers to the image object
        if( beacon.onerror )
        { beacon.onerror = opts.error; }
        if( beacon.onload )
        { beacon.onload  = opts.success; }
 
                // Attach the src for the script call
        beacon.src = opts.url + '?' + qString;
    }
})({
          url : 'http://tcneprod.blob.core.windows.net/collect/hadoop.gif',
          visitorId : {{visitorId}},
          vars : {
		  'hitType':'pageview',
		  'referrer': '"' + document.referrer + '"',
		  'title': document.title,
		  'screenSize': window.innerWidth+'x'+window.innerHeight,
		  'pageType':{{pageTypeURL}},
		  'error':{{error}},
		  'sessionId':{{sessionId}},
		  'experiment':{{experiment}},
		  'membershipId':{{membershipId}},
		  'customerId':{{customerId}},
		  'extrasBookFlightMeal':{{extrasBookFlightMeal}},
		  'loggedIn':{{loggedIn}}
		  }
     });
