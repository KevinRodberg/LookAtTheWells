# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 08:38:52 2017

@author: krodberg
"""

import sys

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QUrl
from PyQt5.QtWebKitWidgets import QWebView

def foo():
        htmlPageTextasList = [
                "<!DOCTYPE html>",
                "<head>",
                "  <title>Google Maps Multiple Markers</title>",
                "<style>",
                "#map {",
                "      width: 100%;",
                "      height: 400px; ",
                "      background-color: grey;",
                "      }",
                "</style>",
                "<title>Google Maps Multiple Markers</title>",
                "</head>",
                "  <body>",
                "    <h3>My Google Maps Demo</h3>",
                "    <div id='map'>",
                "</div>",
                "    <script>",
                "      function avgArr(array,subs) {",
                "              var sum = 0;",
                "              for (var i=0; i < array.length; i++){",
                "                      sum += array[i][subs];",
                "                      }",
                "              return sum/array.length;",
                "              }",
                "      function initMap() {",
                "        var locations = [",
                "                ['Bondi Beach', -33.890542, 151.274856, 5],",
                "                ['Coogee Beach', -33.923036, 151.259052, 4],",
                "                ['Cronulla Beach', -34.028249, 151.157507, 3],",
                "                ['Manly Beach', -33.80010128657071, 151.28747820854187, 2],",
                "                ['Maroubra Beach', -33.950198, 151.259302, 1]",
                "                        ];",
                "        var ctrLat = avgArr(locations,1);",
                "        var ctrLng = avgArr(locations,2);",
                "        var map = new google.maps.Map(document.getElementById('map'), {",
                "          zoom:10,",
                "          center: {lat:ctrLat,lng:ctrLng}",
                "        });",
                "        for (i = 0; i < locations.length; i++) { ",
                "             var uluru = {lat: locations[i][1],lng: locations[i][2]};",
                "             var marker = new google.maps.Marker({",
                "                     position: uluru,",
                "                     map: map",
                "                     });",
                "    }",
                "      }",
                "    </script>",
                "    <script async defer",
                "    src='https://maps.googleapis.com/maps/api/js?key=AIzaSyANOm_b4qDoQXjgtTO3xjfP5VUkDQ9SfOA&callback=initMap'>",
                "    </script>",
                "  </body>",
                "</html>"
        ]
        GoogleHTML = ''.join(htmlPageTextasList)
        return (GoogleHTML)
 
app = QApplication(sys.argv)
 
lat = str(26.6601228)
lng= str(-80.0949537)
satImg=str('&t=k ')
hybridImg=str('&t=h')
cntrOfMap =str('&ll=26.6601228,-80.0949537')
mapSpan=str('&spn=.002,.002')  #span in degrees w.w,h.h
zFactor= str('&z=12')
myHouse='https://maps.google.com/maps/?q='+ lat + ',' + lng+','+zFactor+'@26.6601228,-80.0949537'

browser = QWebView()
browser2 = QWebView()

print myHouse
#browser.load(QUrl(myHouse))
#browser.load(QUrl.fromLocalFile("H:/Documents/Test.html"))
#browser2.load(QUrl.fromLocalFile("K:/untitled2.html"))
text = foo()
browser.setHtml(text)
browser.show()
 
sys.exit(app.exec_())