import ballerina/io;
import ballerina/regex;
import ballerina/log;

type Title record {
    string showId;
    string category;
    string title;
    string director;
    string[] cast;
    string country;
};

type Name record {
    string id;
    string name;
    string birthYear;
    string[] knownForTitles;
};

function searchByName(string query) returns Name[] {
    stream<string, io:Error?>|io:Error namesStream = io:fileReadLinesAsStream("names.tsv");
    if namesStream is stream<string, io:Error?> {
        table<Name> key(id)|error? names = table key(id) from var line in namesStream
            let string[] parts = regex:split(line, io:TAB)
            where parts.length() == 6
            let string id = parts[0],
                string name = parts[1],
                string birthYear = parts[2],
                string[] knownForTitles = regex:split(parts[5], ",")
            where name.includes(query)
            limit 100000
            select {
                id,
                name,
                birthYear,
                knownForTitles
            };

        if names is table<Name> key(id) {
            log:printDebug("Duplicates: ", size = names.length());
            return from var item in names
                select item;
        }
    } else {
        log:printError("Failed to read tsv");
    }

    return [];
}

type Athlete record {
    string name;
    string country;
    string sport;
};

type Medals record {
    int rank;
    string country;
    int gold;
    int silver;
    int bronze;
    int totalMedals;
};

function searchNetflix(string query) returns error? {
    Athlete[] athletes;
    stream<string, io:Error?>|io:Error athletesStream = io:fileReadLinesAsStream("archive/Athletes.csv");
    if athletesStream is stream<string, error?> {
        Athlete[]|error? names = from var line in athletesStream
            let string[] parts = regex:split(line, io:TAB)
            where parts.length() == 3
            let string name = parts[0],
                string country = parts[1],
                string sport = parts[2]
            select {
                name,
                country,
                sport
            };

        if names is Athlete[] {
            log:printInfo("Athletes size: ", size = names.length());
            athletes = names;
        } else {
            log:printError("Failed to query", 'error = names);
            return;
        }
    } else {
        log:printError("Failed to read tsv", 'error = athletesStream);
        return;
    }

    log:printInfo("Reading medals data");
    Medals[] medals;
    stream<string, io:Error?>|io:Error medalsStream = io:fileReadLinesAsStream("archive/Medals.csv");
    if medalsStream is stream<string, io:Error?> {
        Medals[]|error? names = from var line in medalsStream
            let string[] parts = regex:split(line, io:TAB)
            where parts.length() == 7
            where int:fromString(parts[0]) is int
            let int rank = check int:fromString(parts[0])
            let string country = parts[1]
            let int gold = check int:fromString(parts[2])
            let int silver = check int:fromString(parts[3])
            let int bronze = check int:fromString(parts[4])
            let int totalMedals = check int:fromString(parts[5])
            select {
                rank,
                country,
                gold,
                silver,
                bronze,
                totalMedals
            };

        if names is Medals[] {
            log:printInfo("Medals entry count: ", size = names.length());
            medals = names;
        } else {
            log:printError("Failed to query", 'error = names);
            return;
        }
    } else {
        log:printError("Failed to read tsv", 'error = medalsStream);
        return;
    }

    map<int> athletesByCountry = {};
    error? err = from var athlete in athletes
        do {
            int? count = athletesByCountry[athlete.country];
            athletesByCountry[athlete.country] = count is int ? (count + 1) : 1;
        };

    if err is error {
        log:printError("Unable to calculate athletes by country", err);
        return;
    }

    err = from var country in athletesByCountry.keys()
        let int count = athletesByCountry.get(country)
        order by count descending
        do {
            io:println(country, "\t", count);
        };

    if err is error {
        log:printError("Unable to calculate athletes by country", err);
        return;
    }

    map<int> medalsByCountry = {};
    err = from var medal in medals
        do {
            medalsByCountry[medal.country] = medal.totalMedals;
        };

    if err is error {
        log:printError("Unable to calculate medals by country", err);
        return;
    }

    io:println("\n");
    err = from var country in medalsByCountry.keys()
        let int count = medalsByCountry.get(country)
        order by count descending
        do {
            io:println(country, "\t", count);
        };

    if err is error {
        log:printError("Unable to calculate medals by country", err);
        return;
    }

    io:println("\nMedal ratios: ");
    err = from var country in athletesByCountry.keys()
        join var medalStats in medals on country equals medalStats.country
        let decimal goldRatio = (<decimal>medalStats.gold / <decimal>athletesByCountry.get(country)) * 100
        let decimal medalRatio = (<decimal>medalStats.totalMedals / <decimal>athletesByCountry.get(country)) * 100
        order by medalRatio descending
        do {
            io:println(country, "\t", medalRatio, "%");
        };

    if err is error {
        log:printError("Error calculating ratios", err);
    }

    // Athletes by event
    map<int> athletesBySport = {};
    err = from var athlete in athletes
        do {
            int? count = athletesBySport[athlete.sport];
            athletesBySport[athlete.sport] = count is int ? (count + 1) : 1;
        };

    if err is error {
        log:printError("Unable to calculate athletes by sport", err);
        return;
    }

    io:println("\nAthletes by sport:");
    err = from var sport in athletesBySport.keys()
        let int count = athletesBySport.get(sport)
        order by count descending
        do {
            io:println(sport, "\t", count);
        };
}

public function main() {
    log:printInfo("Running...");
    error? err = searchNetflix("");
    if err is error {
        log:printError("Failed", err);
    }
}
