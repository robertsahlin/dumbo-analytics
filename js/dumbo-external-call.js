
(function(i,s,o,g,r,a,m){
i[r]=i[r]||function(){(i[r].q=i[r].q||[]).push(arguments)};
a=s.createElement(o),m=s.getElementsByTagName(o)[0];
a.async=1;a.src=g;m.parentNode.insertBefore(a,m)})
(window,document,'script','http://tcnedev.blob.core.windows.net/analytics/dumbo-dev.js?v=0.01','dumbo');

dumbo({
 'visitorId':{{visitorId}},
 'hitType':'pageview',
 'referrer': '"'+ document.referrer + '"',
 'title': document.title,
 'screenSize': window.innerWidth+'x'+window.innerHeight,
 'pageType':{{pageTypeURL}},
 'error':{{error}},
 'sessionId':{{sessionId}},
 'experiment':{{experiment}},
 'membershipId':{{membershipId}},
 'customerId':{{customerId}}
});

