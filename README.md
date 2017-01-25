# beehive

## Introduction
This nodejs application merges two instances of OpenMRS databases into one. It
works by connection to the two databases and move data from designated source
database into designated destination database. Once run successfully, the
designated destination database will be a merger of the two initial instances.

**Note:**
1. If you still want the original copy of the destination database, then
   you have to create a copy before running this application on it.
2. The assumption is made that the two OpenMRS instance share the same
    concepts, forms.
3. For the purpose of this implementation, some of the tables are purposely ignored
   (See below for the list of forms included)

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

Once the configuration file is in place, run the application with command:

```shell
$ npm install
$ node --harmony orchestrator.js
```
