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
mongoimport --db university --collection Student_counseling --type csv --file "/Users/samanthayeep/ads_assignment2/univerity/Student_Counceling_Information.csv" --headerline
mongoimport --db university --collection Student_performance --type csv --file "/Users/samanthayeep/ads_assignment2/univerity/Student_Performance_Data.csv" --headerline

--show documents in db--
db.Department_info.countDocuments()
db.Employee_info.countDocuments()
db.Student_counseling.countDocuments()
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

db.Student_counseling.find().pretty()

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

--Create a compound index on Student_ID and Paper_ID in Student_performance collection
db.Student_performance.createIndex({ Student_ID: 1, Paper_ID:1 })

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

-------------------------------------------------------------------------------------------------

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

---------------------------------------------------------------------------------------------
--Aggregation--
--2.2.1 count number of employees in each department--
db.Employee_info.aggregate([
    { $group: { _id: "$Department_ID", count: { $sum: 1 } } }]);


--2.2.2 calculates the total marks and average marks for each student
var mapFunction = function() {
    emit(this.Student_ID, { totalMarks: this.Marks, paper_count: 1 });
};
var reduceFunction = function(key, values) {
    var reducedValue = { totalMarks: 0, paper_count: 0 };
    
    values.forEach(function(value) {
        reducedValue.totalMarks += value.totalMarks;
        reducedValue.paper_count += value.paper_count;
    });
    
    return reducedValue;
};
var finalizeFunction = function(key, reducedValue) {
    reducedValue.averageMarks = reducedValue.totalMarks / reducedValue.paper_count;
    return reducedValue;
};
db.Student_performance.mapReduce(
    mapFunction,
    reduceFunction,
    {
        finalize: finalizeFunction,
        out: "total_and_average_marks_per_student"
    }
);
db.total_and_average_marks_per_student.find().pretty();

-- 2.2.3 calculates the number of students for each department
var mapStudents = function() {
    emit(this.Department_Admission, 1);
};
var reduceStudents = function(key, values) {
    return Array.sum(values);
};
db.Student_counseling.mapReduce(
    mapStudents,
    reduceStudents,
    {
        out: "student_count_per_department"
    }
);
db.student_count_per_department.find().pretty()

-- 2.2.3 calculates the number of professors for each department
var mapProfessors = function() {
    emit(this.Department_ID, 1);
};
var reduceProfessors = function(key, values) {
    return Array.sum(values);
};
db.Employee_info.mapReduce(
    mapProfessors,
    reduceProfessors,
    {
        out: "professor_count_per_department"
    }
);
db.professor_count_per_department.find().pretty()

-- 2.2.3 Aggregation query to find the number of professors and students for each department
db.student_count_per_department.aggregate([
    {
        $lookup: {
            from: "professor_count_per_department",
            localField: "_id",
            foreignField: "_id",
            as: "professor_data"
        }
    },
    {
        $unwind: "$professor_data"
    },
    {
        $project: {
            _id: 0,
            department: "$_id",
            student_count: "$value",
            professor_count: "$professor_data.value"
        }
    }
]).pretty()

-- 2.2.4 Find the average marks of each department in descending order
db.Student_performance.aggregate([
    {
        $lookup: {
            from: "Student_counseling",
            localField: "Student_ID",
            foreignField: "Student_ID",
            as: "counseling_info"
        }
    },
    {
        $unwind: "$counseling_info"
    },
    {
        $group: {
            _id: "$counseling_info.Department_Admission", // Group by department ID from counseling_info
            total_marks: { $sum: "$Marks" },
            student_count: { $sum: 1 },
            average_marks: { $avg: "$Marks" }
        }
    },
    {
        $lookup: {
            from: "Department_info",
            localField: "_id",
            foreignField: "Department_ID",
            as: "department_info"
        }
    },
    {
        $unwind: "$department_info"
    },
    {
        $project: {
            _id: 0,
            department: "$department_info.Department_Name",
            total_marks: 1,
            student_count: 1,
            average_marks: 1
        }
    },
    { 
        $sort: { average_marks: -1 } // Sort by average marks in descending order
    }
]).pretty();


--2.2.5 find the top 5 popular paper based on number of student enrolled
db.Student_performance.aggregate([
    {
        $group: {
            _id: "$Paper_ID",
            student_count: { $sum: 1 }
        }
    },
    {
        $lookup: {
            from: "Paper_info",
            localField: "_id",
            foreignField: "Paper_ID",
            as: "paper_info"
        }
    },
    {
        $unwind: "$paper_info"
    },
    {
        $project: {
            _id: 0,
            Paper_ID: "$_id",
            Paper_Name: "$paper_info.Paper_Name",
            student_count: 1
        }
    },
    {
        $sort: { student_count: -1 }
    },
    {
        $limit: 5
    }
]).pretty();

--2.2.6 find the highest and lowest marks per department--
db.Student_performance.aggregate([
    {
        $lookup: {
            from: "Student_counseling",
            localField: "Student_ID",
            foreignField: "Student_ID",
            as: "counseling_info"
        }
    },
    {
        $unwind: "$counseling_info"
    },
    {
        $group: {
            _id: "$counseling_info.Department_Admission",
            max_marks: { $max: "$Marks" },
            min_marks: { $min: "$Marks" }
        }
    },
    {
        $lookup: {
            from: "Department_info",
            localField: "_id",
            foreignField: "Department_ID",
            as: "department_info"
        }
    },
    {
        $unwind: "$department_info"
    },
    {
        $project: {
            _id: 0,
            department: "$department_info.Department_Name",
            max_marks: 1,
            min_marks: 1
        }
    },
    { 
        $sort: { department: 1 } // Sort by department name alphabetically
    }
]).pretty();

--2.2.7 find the pass rate for each department
db.Student_performance.aggregate([
    {
        $lookup: {
            from: "Student_counseling",
            localField: "Student_ID",
            foreignField: "Student_ID",
            as: "counseling_info"
        }
    },
    {
        $unwind: "$counseling_info"
    },
    {
        $group: {
            _id: "$counseling_info.Department_Admission",
            total_students: { $sum: 1 },
            passed_students: { $sum: { $cond: { if: { $gte: ["$Marks", 50] }, then: 1, else: 0 } } }
        }
    },
    {
        $project: {
            _id: 0,
            department: "$_id",
            pass_rate: { $multiply: [{ $divide: ["$passed_students", "$total_students"] }, 100] }
        }
    },
    {
        $lookup: {
            from: "Department_info",
            localField: "department",
            foreignField: "Department_ID",
            as: "department_info"
        }
    },
    {
        $unwind: "$department_info"
    },
    {
        $project: {
            department: "$department_info.Department_Name",
            pass_rate: { $round: ["$pass_rate", 2] }
        }
    },
    { 
        $sort: { pass_rate: -1 } // Sort by pass rate in descending order
    }
]).pretty();



--2.2.8 analyze the distribution of marks across different grade level
var mapGrade = function() {
    emit(this.Student_ID, {
        grade: this.Marks >= 85 ? 'A' : this.Marks >= 70 ? 'B' : this.Marks >= 50 ? 'C' : 'D'
    });
};

var reduceGrade = function(key, values) {
    var result = { A: 0, B: 0, C: 0, D: 0 };
    values.forEach(function(value) {
        result[value.grade]++;
    });
    return result;
};

db.Student_performance.mapReduce(
    mapGrade,
    reduceGrade,
    {
        out: "grade_distribution"
    }
);

db.grade_distribution.find().pretty();

--2.2.9 Identify Students with Over 80% Improvement in Average Marks--
db.Student_performance.aggregate([
    {
        $group: {
            _id: "$Student_ID",
            allMarks: { $push: "$Marks" }
        }
    },
    {
        $addFields: {
            firstAverageMarks: { $avg: { $slice: ["$allMarks", 0, 1] } },
            lastAverageMarks: { $avg: { $slice: ["$allMarks", -1, 1] } }
        }
    },
    {
        $addFields: {
            improvementPercent: {
                $cond: {
                    if: { $ne: ["$firstAverageMarks", 0] },
                    then: { $multiply: [{ $divide: [{ $subtract: ["$lastAverageMarks", "$firstAverageMarks"] }, "$firstAverageMarks"] }, 100] },
                    else: 0
                }
            }
        }
    },
    {
        $match: {
            improvementPercent: { $gte: 80 },
            lastAverageMarks: { $gte: 60 }
        }
    },
    {
        $lookup: {
            from: "Student_counseling",
            localField: "_id",
            foreignField: "Student_ID",
            as: "counseling_info"
        }
    },
    { $unwind: "$counseling_info" },
    {
        $project: {
            _id: 0,
            Student_ID: "$_id",
            Department: "$counseling_info.Department_Admission",
            ImprovementPercent: "$improvementPercent"
        }
    },
    { $sort: { ImprovementPercent: -1 } }
]).forEach(printjson);
