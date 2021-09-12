import ballerina/io;
import ballerina/regex;

# Represents an athlete
#
# + name - Name of the athlete  
# + country - Country of the athlete 
# + sport - Sport to which athlete participated  
type Athlete record {
    string name;
    string country;
    string sport;
};

# Represents number of medals won by a country
#
# + rank - Rank by number of gold medals won 
# + country - Name of the country
# + gold - Number of gold medals
# + silver - Number of silver medals
# + bronze - Number of bronze medals
# + totalMedals - Total number of medals won 
type MedalStats record {
    int rank;
    string country;
    int gold;
    int silver;
    int bronze;
    int totalMedals;
};

type CountryRatio record {
    string country;
    float ratio;
};

# Get medals stats as a record stream
# + return - Stream of MedalStats or an error  
function getMedalStats() returns stream<MedalStats, error?>|error {
    stream<string[], io:Error?>|error tsvStream = readTsvAsStream("data/Medals.tsv");
    if tsvStream is stream<string[], io:Error?> {
        return stream from var entry in tsvStream
            where entry.length() == 7
            where int:fromString(entry[0]) is int
            let int rank = check int:fromString(entry[0]),
                string country = entry[1],
                int gold = check int:fromString(entry[2]),
                int silver = check int:fromString(entry[3]),
                int bronze = check int:fromString(entry[4]),
                int totalMedals = check int:fromString(entry[5])
            select {
                rank,
                country,
                gold,
                silver,
                bronze,
                totalMedals
            };
    } else {
        return error("Failed to read medals data", tsvStream);
    }
}

# Get athletes stream from the TSV
# + return - Stream of athlete records or an error 
function getAthletes() returns stream<Athlete, error?>|error {
    stream<string[], io:Error?>|error athletesStream = readTsvAsStream("data/Athletes.tsv");
    if athletesStream is stream<string[], io:Error?> {
        return stream from var entry in athletesStream
            where entry.length() == 3
            where entry[0] != "Name"
            let string name = entry[0],
                string country = entry[1],
                string sport = entry[2]
            select {
                name,
                country,
                sport
            };
    } else {
        return error("Unable to read athletes", athletesStream);
    }
}

# Finds and prints the top 10 gold medal winning countries in Olympics 2020
# + return - An error if any error occurs during processing.
function findTop10MedalWinners() returns error? {
    string[]|error? countries = from var stats in check getMedalStats()
        order by stats.gold descending
        limit 10
        select stats.country;

    if countries is string[] {
        io:println("Top 10 winners: ");
        foreach var country in countries {
            io:println("\t" + country);
        }
    } else {
        io:println("Failed to query top medal winners", countries);
    }
}

# Return a map of number of athletes against country
# + return - Map of number of athletes against country
function findNumOfAthletesByCountry() returns map<int>|error {
    map<int> countByCountry = {};
    check from var athlete in check getAthletes()
        do {
            countByCountry[athlete.country] = (countByCountry.hasKey(athlete.country) ? countByCountry.get(athlete.country) : 0) + 1;
        };
    return countByCountry;
}

# Find top 10 countries by number of athletes participated
# + return - Error if any error occurs
function findTop10CountriesByAthletes() returns error? {
    map<int> athletesByCountry = check findNumOfAthletesByCountry();
    string[] top10Countries = from [string, int] entry in athletesByCountry.entries()
        order by entry[1] descending
        limit 10
        select entry[0];

    io:println("Top 10 countries by number of Athletes: ");
    foreach var country in top10Countries {
        io:println("\t", country);
    }
}

# Find countries with best gold medals won/number of athletes participated ratio
# + return - Error if any occurs
function findTop10GoldMedalsToAthletesRatio() returns error? {
    map<int> athletesByCountry = check findNumOfAthletesByCountry();
    stream<MedalStats, error?> medalStats = check getMedalStats();

    CountryRatio[] ratios = from [string, int] athletesCount in athletesByCountry.entries()
        join var stats in medalStats on athletesCount[0] equals stats.country
        let float ratio = (<float>stats.gold / <float>athletesCount[1]) * 100
        order by ratio descending
        limit 10
        select {
            country: athletesCount[0],
            ratio
        };

    io:println("Top 10 countries with gold medals/athletes ratio");
    foreach var ratio in ratios {
        io:println("\t", ratio.country, "\t", float:round(ratio.ratio), "%");
    }
}

# Find countries with best any medal won/number of athletes participated ratio
# + return - Error if any occur
function findTop10AnyMedalToAthletesRatio() returns error? {
    map<int> athletesByCountry = check findNumOfAthletesByCountry();
    stream<MedalStats, error?> medalStats = check getMedalStats();

    CountryRatio[] ratios = from [string, int] athletesCount in athletesByCountry.entries()
        join var stats in medalStats on athletesCount[0] equals stats.country
        let float ratio = (<float>stats.totalMedals / <float>athletesCount[1]) * 100
        order by ratio descending
        limit 10
        select {
            country: athletesCount[0],
            ratio
        };

    io:println("Top 10 countries with any medals/athletes ratio");
    foreach var ratio in ratios {
        io:println("\t", ratio.country, "\t", float:round(ratio.ratio), "%");
    }
}

# Given a tsv, this method returns a stream with each line as a string[]
#
# + filePath - Path to the tsv file
# + return - Return the stream or an error if any error occurs while reading the file or parsing  
function readTsvAsStream(string filePath) returns stream<string[], io:Error?>|error {
    stream<string, io:Error?>|io:Error lineStream = io:fileReadLinesAsStream(filePath);
    if lineStream is stream<string, io:Error?> {
        return stream from var line in lineStream
            let string[] parts = regex:split(line, io:TAB)
            select parts;
    } else {
        return error("Unable to read tsv", 'error = lineStream);
    }
}

public function main() {
    error? err = findTop10MedalWinners();
    if err is error {
        io:println("Failed to execute query", err);
    }

    err = findTop10CountriesByAthletes();
    if err is error {
        io:println("Failed to execute query", err);
    }

    err = findTop10GoldMedalsToAthletesRatio();
    if err is error {
        io:println("Failed to execute query", err);
    }

    err = findTop10AnyMedalToAthletesRatio();
    if err is error {
        io:println("Failed to execute query", err);
    }
}
