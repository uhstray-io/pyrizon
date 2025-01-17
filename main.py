#import the pickfiles python file
#path: pickfiles.py
import pickfiles as pf
import workflow as wf

# Create a pipeline
pipeline = wf.DataPipeline("data_processing")

# Create a source select popup



# Add a data source
source = wf.DataSource(
    name="raw_data",
    connection_string="postgresql://localhost:5432/db",
    type="postgresql"
)
pipeline.add_source(source)

# Define and add tasks
def process_data(data):
    # Processing logic here
    pass

task = wf.Task(
    name="process_raw_data",
    function=process_data,
    dependencies=[]
)
pipeline.add_task(task)

# Execute the pipeline
results = pipeline.execute()