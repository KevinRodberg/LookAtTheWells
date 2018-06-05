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
        part1=["<!DOCTYPE html>",
               "<head>",
                "  <title>Google Maps Multiple Markers</title>",
                "<style>",
                "#map {",
                "      width: 100%;",
                "      height: 600px; ",
                "      background-color: grey;",
                "      }",
                "</style>",
                "<title>Google Maps Multiple Markers</title>",
                "</head>",
                "  <body>",
                "    <h3>My Google Maps Demo</h3>",
                "    <div id='map'>",
                "</div>",
                " <script>"]
        func_avgArr = ["      function avgArr(array,subs) {",
                 "              var sum = 0;",
                 "              for (var i=0; i < array.length; i++){",
                 "                      sum += array[i][subs];",
                 "                      }",
                 "              return sum/array.length;",
                 "              }"]
        func_initMap1 =["      function initMap() {"]
         
        locations = ["        var locations = [",
                "                ['Cole and Danielle', 26.65925, -80.0937, 2],",
                "                ['Our House', 26.6601228, -80.0949537, 1]",
                "                        ];"]
         
        fun_initMap2=["        var ctrLat = avgArr(locations,1);",
                "        var ctrLng = avgArr(locations,2);",
                "        var map = new google.maps.Map(document.getElementById('map'), {",
                "          zoom: 19,mapTypeId: 'satellite',",
                "          center: {lat:ctrLat,lng:ctrLng}",
                "        });",
                "        for (i = 0; i < locations.length; i++) { ",
                "             var uluru = {lat: locations[i][1],lng: locations[i][2]};",
                "             var marker = new google.maps.Marker({",
                "                     position: uluru,",
                "                     map: map",
                "                     });",
                "    }",
                "      }"]
        part2 =["    </script>",
                "    <script async defer",
                "    src='https://maps.googleapis.com/maps/api/js?key=AIzaSyANOm_b4qDoQXjgtTO3xjfP5VUkDQ9SfOA&callback=initMap'>",
                "    </script>",
                "  </body>",
                "</html>"]
          
        htmlPageTextasList = part1 + func_avgArr + func_initMap1 + locations + fun_initMap2+ part2

        GoogleHTML = ''.join(htmlPageTextasList)
        return (GoogleHTML)
    
if __name__ == '__main__':
    app = QApplication(sys.argv)
 
    browser = QWebView()

    text = foo()
    browser.setHtml(text)
    browser.show()
 
    sys.exit(app.exec_())


