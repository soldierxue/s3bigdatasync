
# Modules:
## Module 0 – Leo (Done): ListProducer/
    Generate Bucket Objects List
    Input:
        Bucket

    Output: 
        Object List(json)

## Module I - Jason: TaskProducer/
    Generate Tasks
    Input:
        Json Object List
        Job List(Failed Task)
    Output:
        Job Queues
        Tasks Stats
        Estimated Cost
        DryRun

## Module II - Leo: TaskExecutor/
    Execute Tasks
    Input:
        Job Queues
    Output:
        Job Status
        Estimated Cost

## Module III - Leo: TaskMonitor/
    Monitor Task Progress
    Input:
        Jobs / Jobs Status
    Output:
        Stat
        Jobs Status

## Module IV - Jason: ObjectVerification/
    Source/Dest Verification
    Input:
        Source Object List
        Dest Object List
    Output:
        Object List(Format)

## Module V - David (Done) : UICenter/
    UI
    Input:
        Data
    Output:
        User interactions
