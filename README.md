# Akvelon Task

Use Apache Beam Python SDK (https://beam.apache.org/)
You should be able to run it locally, don’t need to serve it in the cloud. Jupiter notebook is a
good choice.

We want to process a file and find
- Most common word
- Least common word

Please notice that counting should be case insensitive.
Input and Output paths should be parameters but some default values should be provided.

## Example

input.txt:
aa Ab ab Ab C CD

most_common.txt
ab

least_common.txt
aa
—
(c and cd are also valid outputs


## Instructions

### Prerequirements

To run this script locally, you only must to have Python 3+ and pip 2.7+ installed. To check this you can run:

- python --version

- pip --version

### Aditiona pip packages

After check your python and pip version, could be good install apache beam for this task, we only have to run:

- pip install apache-beam

Note: It is a good idea to do this and work your projects into a virtual envieronment using venv or pyenv and pipenv

### Run the task

If you have everything ready for run an apache beam script you should to download the file commonwords.py locally

https://github.com/dddaniel01/coding/blob/main/commonwords.py

Then, you only should to open a terminal and run the follow command:

- python -m /path/commonwords --input /path/to/inputfile --output /path/to/write/counts

Note: Input and oputput parameters are optional, default values are letter.txt as input and outputs/ for output directory

If the script run correctly you are going to have tow files as oput in your ouput directory: 
- most_common.txt
- least_common.txt

