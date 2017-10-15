package com.beyondbasics

import EasyJson._ // Import the library:

object parseJSON extends App{
    val data1 = EasyJson.load("example.json");

    // Or from a string
    val data2 = EasyJson.loads("{\"test\": 123}");

    // Examine the data
    assert(data2.select("test").as[Int] * 2 == 246);

    // Save the data to a file
//    EasyJson.dump("result.json", data2);

    // Or to a string
    val text_data_1 = EasyJson.dumps(data1);

    // Minify a json object
    assert(EasyJson.dumps(EasyJson.loads("[1, 2, 3]")) == "[1,2,3]");
  
}