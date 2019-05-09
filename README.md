# beehive

## Introduction
This nodejs application merges two instances of OpenMRS databases into one. It
works by connecting to the two databases and move data from designated source
database into designated destination database. Once run successfully, the
designated destination database will be a merger of the two initial instances.

**Note:**
1. If you still want the original copy of the destination database, then
   you have to create a copy before running this application on it.
2. The assumption is made that the two OpenMRS instances share the same
    metadata (i.e. concepts, forms e.t.c).
3. For the purpose of this implementation, some of the tables are purposely ignored
   (See below for the list of tables included)

## Tables
Below is the list of tables whose records are moved.
1. *person*

2. *person_attribute_type*

3. *person_attribute*

4. *person_name*

5. *person_address*

6. *relationship_type*

7. *relationship*

8. *patient*

9. *patient_identifier_type*

10. *patient_identifier*

11. *users*

12. *role*

13. *role_role*

14. *privilege*

15. *role_privilege*

16. *user_role*

17. *location*

18. *provider*

19. *provider_attribute_type*

20. *provider_attribute*

21. *visit*

22. *visit_type*

23. *encounter*

24. *encounter_role*

25. *encounter_provider*

26. *obs*

27. *program*

28. *program_workflow*

29. *program_workflow_state*

30. *patient_state*

31. *GAAC module tables*

## Requirements
* nodejs 7+

## Running
Clone the code from github.

`$ git clone https://github.com/mhawila/beehive.git`

Change into the project directory, and create a JSON configuration file called
*config.json* putting the following content.
```javascript
{
    "source": {
        "host": "mysql source host or IP",
        "username": "username",
        "password": "secret",
        "openmrsDb": "openmrs1",
        "location": "unique string to identify location"    //Must be provided.
    },
    "destination": {
        "host": "mysql destination host or IP",
        "username": "username",
        "password": "secret",
        "openmrsDb": "openmrs2"
    },
    "batchSize": 16000,
    "generateNewUuids": true,       //Must be provided.
    "debug": false
}
```
### Explanation of configuration options
* _batchSize_: This is number of records that will be moved at a time.
* _generateNewUuids_: Whether to generate UUIDs or not. If you want to move the
               records with existing UUID which is recommended set this to false
               **note:** This option has to be explicitly provided.
* _debug_: Whether to print debug level statements.

**Note:** Substitute the given values with appropriate values.

Once the configuration file is in place, Install the required dependencies:

```shell
$ npm install
```

Run the application
```shell
$ node --harmony orchestrator.js
```
Running without committing changes to the database (Dry running)
```shell
$ node --harmony orchestrator.js --dry-run
```

### Memory Issues.
This application can be very memory intensive because it reads large sections of tables into memory.
As a result because the default memory limit in Node.js is 512 MB, sometimes the user might experience the `Javascript heap out of memory` error when merging big databases.

V8 which Node.js is based provided an option `--max_old_space_size=Y` where `Y` is the number interpreted as
megabytes. You can use this option to specify more memory. The example below shows node run with max 2GB of memory allocated to it.

```shell
$ node --harmony --max_old_space_size=2048 orchestrator.js
```
