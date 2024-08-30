-- This function calculates the total marks and average marks for each student
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

-- This function calculates the number of students for each department
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
-- This function calculates the number of professors for each department
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

-- Aggregation query to find the number of professors and students for each department
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

-- Find the average marks of each department in descending order
// db.Student_performance.aggregate([
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
