--create database--
use university

--delete db--
db.dropDatabase()


--create collection--
db.createCollection("Department_info")
db.createCollection("Employee_info")
db.createCollection("Student_counseling")
db.createCollection("Student_performance")
db.createCollection("Paper_info");

--import csv into the db--
mongoimport --db university --collection Department_info --type csv --file "/Users/samanthayeep/ads_assignment2/univerity/Department_Information.csv" --headerline
mongoimport --db university --collection Employee_info --type csv --file "/Users/samanthayeep/ads_assignment2/univerity/Employee_Information.csv" --headerline
mongoimport --db university --collection Student_counselling --type csv --file "/Users/samanthayeep/ads_assignment2/univerity/Student_Counceling_Information.csv" --headerline
mongoimport --db university --collection Student_performance --type csv --file "/Users/samanthayeep/ads_assignment2/univerity/Student_Performance_Data.csv" --headerline

--show documents in db--
db.Department_info.countDocuments()
db.Employee_info.countDocuments()
db.Student_counselling.countDocuments()
db.Student_performance.countDocuments()
db.Paper_info.countDocuments()

--migrate paper_name from Student_performance to Paper_info--
db.Student_performance.distinct("Paper_ID").forEach(function(paper_id) {
    var paper = db.Student_performance.findOne({ Paper_ID: paper_id });
    if (paper) {
        db.Paper_info.update(
            { Paper_ID: paper_id },
            { $set: { Paper_Name: paper.Paper_Name } },
            { upsert: true }
        );
    }
});


--update Student_performance to remove paper_name--
db.Student_performance.updateMany(
    {},
    { $unset: { Paper_Name: "" } }
);


--check data--
db.Department_info.find().pretty()

db.Employee_info.find().pretty()

db.Student_counselling.find().pretty()

db.Student_performance.find().pretty()

db.Paper_info.find().pretty()

--create index--
--Create an index on Department_Name in Department_info collection--
db.Department_info.createIndex({ Department_Name: 1 })

--Create an index on Department_ID in Department_info collection 
db.Department_info.createIndex({ Department_ID: 1 });

--Create an index on Student_ID in Student_performance collection
db.Student_performance.createIndex({ Student_ID: 1 });

--Create an index on Student_ID in Student_counseling collection
db.Student_counseling.createIndex({ Student_ID: 1 });

--Create an index on Employee_ID in Employee_info collection--
db.Employee_info.createIndex({ Employee_ID: 1 })

--Create a compound index on Department_ID and Student_ID in Student_counseling collection
db.Student_counseling.createIndex({ Department_Admission: 1, Student_ID: 1 })

--Create a compound index on Student_ID and Semester_Name in Student_performance collection
db.Student_performance.createIndex({ Student_ID: 1, Semester_Name: 1 })

--validation for data insertion--
--department
db.runCommand({
    collMod: "Department_info",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["Department_ID", "Department_Name", "DOE"],
            properties: {
                Department_ID: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                Department_Name: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                DOE: {
                    bsonType: "date",
                    description: "must be a date and is required"
                }
            }
        }
    },
    validationLevel: "strict"
});

--employee
db.runCommand({
    collMod: "Employee_info",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["Employee_ID", "DOB", "DOJ", "Department_ID"],
            properties: {
                Employee_ID: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                DOB: {
                    bsonType: "date",
                    description: "must be a date and is required"
                },
                DOJ: {
                    bsonType: "date",
                    description: "must be a date and is required"
                },
                Department_ID: {
                    bsonType: "string",
                    description: "must be a string and is required"
                }
            }
        }
    },
    validationLevel: "strict"
});

--student counseling
db.runCommand({
    collMod: "Student_counseling",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["Student_ID", "DOA", "DOB", "Department_Choices", "Department_Admission"],
            properties: {
                Student_ID: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                DOA: {
                    bsonType: "date",
                    description: "must be a date and is required"
                },
                DOB: {
                    bsonType: "date",
                    description: "must be a date and is required"
                },
                Department_Choices: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                Department_Admission: {
                    bsonType: "string",
                    description: "must be a string and is required"
                }
            }
        }
    },
    validationLevel: "strict"
});

--student performance
db.runCommand({
    collMod: "Student_performance",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["Student_ID", "Semester_Name", "Paper_ID", "Marks"],
            properties: {
                Student_ID: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                Semester_Name: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                Paper_ID: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                Marks: {
                    bsonType: "int",
                    minimum: 0,
                    maximum: 100,
                    description: "must be an integer between 0 and 100 and is required"
                }
            }
        }
    },
    validationLevel: "strict"
});

--Paper_info
db.runCommand({
    collMod: "Paper_info",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["Paper_ID", "Paper_Name"],
            properties: {
                Paper_ID: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                Paper_Name: {
                    bsonType: "string",
                    description: "must be a string and is required"
                }
            }
        }
    },
    validationLevel: "strict"
});


--show validations of all collections--
db.getCollectionInfos().forEach(function(collectionInfo) {
    print("Collection: " + collectionInfo.name);
    printjson(collectionInfo.options);
});

--crud(examples)--
db.Department_info.insertOne({
    Department_ID: "IDEPT12345",
    Department_Name: "soft Engineering",
    DOE: new Date("1990-05-12")
});

--retrieve data(search by specific id)--
db.Department_info.find({Department_ID:"IDEPT7783"}).pretty()
db.Employee_info.find({ Employee_ID: "IU366351" }).pretty()
db.Student_counseling.find({ Student_ID: "SID20183160" }).pretty()
db.Student_performance.find({ Student_ID: "SID20183160", Paper_ID: "SEMI0022256" }).pretty()
db.Paper_info.find({ Paper_ID: "SEMI0022256" }).pretty()


--update--
db.Department_info.updateOne(
    { Department_ID: "IDEPT12345" },
    { $set: { Department_Name: "Mechanical and Aerospace Engineering" } }
);

--delete--
db.Department_info.deleteOne({ Department_ID: "IDEPT12345" });

--Aggregation--
--count number of employees in each department--
db.Employee_info.aggregate([
    { $group: { _id: "$Department_ID", count: { $sum: 1 } } }]);

--find average marks for each student in student performance--
db.Student_performance.aggregate([
  { $group: { _id: "$Student_ID", averageMarks: { $avg: "$Marks" } } }
])

