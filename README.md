# MBD project group 4, Where are most websites hosted?
This repository holds the code for MBD Group 4 of 2020.
To give a quick summary, our project was to determine the hosting locations of the most popular websites, using the CommonCrawl dataset.

This was done by matching the IPs in the CommonCrawl with geolocations form the GeoLite dataset.
Some problems we ran into were:
1. Running python code with third party libraries on the cluster
2. Only getting a single executor assigned to our task
3. Not being able to save RDDs as textfiles.

The first problem was easily solved, by closely following instructions which were given on a website that was given on Canvas.
Essentially all that needs to be done is creating a virtual environment which has the libraries installed and passing this onto the cluster when submitting your spark program.

The second problem was not solved because it was not a problem with spark. We had to parse the files from the CommonCrawl to retrieve useful data using an external library. However this job was not really spark compatible, so instead multi-processing was used, do not be afraid to stick to some multi-processing if you are stuck on not getting enough executors.

The last problem was and still is a complete mystery, it would always give an exception that a certain directory was already made even if it was set to overwrite or delete that directory, if anyone finds out why this happens I highly recommend them to put this in their README.


