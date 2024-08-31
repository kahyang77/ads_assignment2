-- find the top 5 popular paper based on number of student enrolled
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

-- find the highest and lowest marks per department--
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

-- find the pass rate for each department
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



-- analyze the distribution of marks across different grade level
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