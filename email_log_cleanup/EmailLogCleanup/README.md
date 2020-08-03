# Lambda Function Email Log Cleanup

###Goal:
The email_log_cleanup project is a standalone AWS Lambda 
function that uses a scheduled trigger to delete old 
email logs on a daily basis. This functions by taking in an environment
variable that determines how many days ago the cutoff will be for deletion.
The function then outputs how many entries have been deleted from each
table.

###Prerequisites:
To use this function, a user will need access to an IAM role
that is conencted to the VolunteerMatch vm_eng AWS account.
That IAM role will likely need a series of permissions including:
- Ability to view and run Lambda functions.
- Ability to view the RDS instance.

If the user wishes to run this lambda function locally, they must setup
an AWS Toolkit for their chosen IDE in conjunction with the SAM CLI, Docker,
and Maven. I built this function in IntelliJ 2020.1 using the AWS Toolkit for Jetbrains found here:
[IntelliJ](https://docs.aws.amazon.com/toolkit-for-jetbrains/latest/userguide/welcome.html).


For more information on using an AWS Toolkit see the AWS README under email_log_cleanup/events.

###Function Execution:

The best way to see the function execution is online in the AWS
console.
1. The first step is to login to the console with your IAM role connected
to the VolunteerMatch account. 
2. Under "Services", click Lambda, or search for it in the search bar.
3. The name of the function is "dev-emailcleanup".
4. Once on the function page you will see under the "Designer" pane
that the EventBridge triggers the function. The EventBridge trigger consists
of a rule that can be changed, and it determines how often the function runs.
To change this, simply disable the EventBridge "dayRule" and create a new
rule with the desired schedule specifications.

You may also manually test the function (regardless of the schedule). At the top
of the function page there is a test button. To use it you are required to
create a test event. Because the function does not use the event input value, 
you may give it an arbitrary object input. Use the default input event:
```bash
{
  "key1": "value1",
  "key2": "value2",
  "key3": "value3"
}
```
If you wish to alter the cutoff date for the email log deletion, just
change the environment variable "OFFSET" to the desired integer. 
- Ex: If the OFFSET is 365, any email older than 1 year will be deleted.

###How to Contribute:

For any further contribution to this project or repository, do the following:
1. Fork this repository and clone the fork to the desired location on your computer.
2. Create a new branch: `git checkout -b new_branch`
3. Create new upstream repo for the branch: `git remote add upstream https://github.com/volunteermatch/email-stats.git`
4. Make desired changes to your branch and commit them.
5. Push committed changes to new branch: `git push -u origin new_branch`
6. Create your pull request.

###Author:
- @smashed-toes
