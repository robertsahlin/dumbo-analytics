var HadoopAnalyticsQueue = function () {
	this.push = function () {
		for (var i = 0; i < arguments.length; i++) try {
			if (typeof arguments[i] === "function") arguments[i]();
			else {
				for (var j = 0; j < arguments[i].length; j++) try {
					console.log(arguments[i][j]);
					var opts = arguments[i][j] || {};
					var url = 'http://tcnedev.blob.core.windows.net/collect/dumbo.gif';
					opts.cb = Math.floor(Math.random() * 10e12);
					opts.domain = document.location.hostname;
					opts.page = document.location.pathname;
					window.dumbo.q.hid = window.dumbo.q.hid || opts.cb;
					opts.hid = window.dumbo.q.hid;
					console.log("hid:" + opts.hid);	
						
					// Split up vars object into an array
					var varsArray = [];
					//for(var key in opts.vars){ 
					for(var key in opts){ 
						if(opts[key] !== undefined){
						varsArray.push(encodeURIComponent(key)+'='+encodeURIComponent(opts[key])); 
					    }
					}
					// Build query string
					var qString = varsArray.join('&');
					// Create a beacon if a url is provided
					// Create a brand NEW image object
					var beacon = document.createElement("img");
					// Attach the event handlers to the image object
					if( beacon.onerror ){ beacon.onerror = function(){}; }
					if( beacon.onload ){ beacon.onload  = function(){}; }
					// Attach the src for the script call
					beacon.src = url + '?' + qString;
				}catch (e) {}
			}
        } catch (e) {}
    };
};

(function(){
// get the existing window.dumbo.q array
var _old_dumbo_q = window.dumbo.q;
// create a new window.dumbo.q object
window.dumbo.q = new HadoopAnalyticsQueue();
// execute all of the queued up events - apply() turns the array entries into individual arguments
window.dumbo.q.push.apply(window.dumbo.q, _old_dumbo_q);
})();