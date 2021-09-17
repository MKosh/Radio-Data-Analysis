using Dates
using HTTP, SQLite, DataFrames, JSONTables, Plots

################################################################################
### DataFrame:
function InitializeDF(newDF, dbFile="radio.db", dbTable="tracks")
    if (newDF)
        source = HTTP.get("https://nowplaying.bbgi.com/WMMRFM/list?limit=1000&amp;offset=0")
        information = String(source.body)
        jt = jsontable(information)
        df = DataFrame(jt)
        return df
    else
        db = CreateDatabase(dbFile)
        command = string("SELECT * FROM ", dbTable)
        df = DBInterface.execute(db, command) |> DataFrame
        return df
    end
end

################################################################################
### DataFrame:
function InitialDFClean(df)
    select!(df,Not(:id))
    select!(df,Not(:site))
    select!(df,Not(:station))

    insertcols!(df, 3, :date_played => df.createdOn)
    insertcols!(df, 4, :time_played => df.createdOn)

    df.date_played = SubString.(df.date_played, 1,10)
    df.time_played = SubString.(df.time_played, 12,23)
    return nothing
end

################################################################################
### SQL:
function CreateDatabase(DBname::String)
    db = SQLite.DB(DBname)
    return db
end

################################################################################
### SQL:
function CreateTable(db, tableName::String)
    command = string("CREATE TABLE IF NOT EXISTS ", tableName, "(artist TEXT, title TEXT,
        date_played TEXT, time_played TEXT, timestamp INTEGER, createdOn TEXT)")
    DBInterface.execute(db, command)
    return nothing
end

################################################################################
### SQL:
function DropTable(db, tableName::String)
    command = string("DROP TABLE IF EXISTS ", tableName)
    DBInterface.execute(db, command)
    return nothing
end

################################################################################
### SQL:
function FillDatabase(df, db, tableName::String)
    for i in 1:nrow(df)
        artist_i = replace(df.artist[i], "'" => "")
        title_i = replace(df.title[i], "'" => "")
        command = string("INSERT INTO ", tableName, " VALUES(", "'",artist_i, "','", 
            title_i, "','", df.date_played[i], "','", df.time_played[i], "',", 
            df.timestamp[i], ",'", df.createdOn[i], "')")
        DBInterface.execute(db, command)
    end
    return nothing
end

################################################################################
### SQL:
function CountPlays(db, tableName)
    command = string("SELECT",
                        " artist,",
                        " title,",
                        " COUNT(*) AS count",
                    " FROM",
                        " ", tableName,
                    " GROUP BY",
                        " artist,",
                        " title")
    playCounts = DBInterface.execute(db, command) |> DataFrame
    show(playCounts)
    println("")
    return playCounts
end

################################################################################
### SQL:
function MostPlayedSongs(db, tableName, limit)
    command = string("SELECT",
                        " artist,",
                        " title,",
                        " COUNT(*) AS count",
                    " FROM",
                        " ", tableName,
                    " GROUP BY",
                        " artist,",
                        " title",
                    " ORDER BY",
                        " count DESC",
                    " LIMIT ", limit)
    counts = DBInterface.execute(db, command) |> DataFrame
    show(counts)
    println("")
    return counts
end

################################################################################
### SQL:
function MostPlayedArtists(db, tableName, limit)
    command = string("SELECT",
                        " artist,",
                        " COUNT(*) AS count",
                    " FROM",
                        " ", tableName,
                    " GROUP BY",
                        " artist",
                    " ORDER BY",
                        " count DESC",
                    " LIMIT ", limit)
    counts = DBInterface.execute(db, command) |> DataFrame
    show(counts)
    println("")
    return counts
end

################################################################################
### SQL:
function DeleteDups(db, tableName::String)
    command = string("DELETE",
                    " FROM",
                        " tracks",
                    " WHERE",
                        " rowid NOT IN (",
                            " SELECT min(rowid)",
                            " FROM",
                                " ", tableName,
                            " GROUP BY",
                                " artist,",
                                " title,",
                                " timestamp)")
    DBInterface.execute(db, command)
    return nothing
end

################################################################################
### SQL: Get times played for a band
#test = DBInterface.execute(database, "SELECT time_played, COUNT(*) AS count 
#FROM tracks WHERE artist='Foo Fighters' GROUP BY time_played ORDER BY count DESC LIMIT 10") |> DataFrame
function ArtistPlaysByTime(db, tableName::String, Artist::String)
    command = string("SELECT",
                        " time_played,",
                        " COUNT (*) AS count",
                    " FROM",
                        " tracks",
                    " WHERE",
                        " artist='Foo Fighters'",
                    " GROUP BY",
                        " time_played")
    df = DBInterface.execute(db, command) |> DataFrame
    return df
end

################################################################################
### Plots:
function barPlot(df, x, y, column, limit)
    plotTitle = string("Top ", limit, " ", names(df, column)[1], " Played")
    println("\n", "Plots", " "^6, ": Plotting ", plotTitle)
    bar(x, y, xticks=:all, xrotation=45, ylabel="Counts", title=plotTitle,
        legend=false, size=(1000,800))
    gui()
    return nothing
end

################################################################################
### Plots:86399000000000, 88199000000000,
function histoTimePlot(df, x)
    histogram(Dates.Time.(x, "HH:MM:SS.sss"), bins=0:3.6e12:9.0e13, xrotation=45, xlims=(0, 89999000000000), legend=false)
    gui()
    return nothing
end

################################################################################
###

databaseFile   = "radio.db"
table          = "tracks"
num_entries    = 25
gettingNewData = false

if (isfile(databaseFile) && !gettingNewData)
    println("\n","SQL"," "^8, ": Opening Database: ", databaseFile)
    database = CreateDatabase(databaseFile)
    println(" "^11, ": Creating table: ", table, " if it doesn't already exist")
    CreateTable(database, table)
    println("DataFrame"," "^2, ": Creating a dataframe from ", databaseFile)
    dataframe = InitializeDF(false, databaseFile, table)
elseif (gettingNewData)
    println("\n", "DataFrame", " "^2, ": Pulling data and initializing the dataframe")
    dataframe = InitializeDF(true)
    InitialDFClean(dataframe)
    println("SQL"," "^8, ": Opening Database: ", databaseFile)
    database = CreateDatabase(databaseFile)
    println(" "^11, ": Creating table: ", table, " if it doesn't already exist")
    CreateTable(database, table)
    println(" "^11, ": Filling the database from the dataframe and removing dups")
    FillDatabase(dataframe, database, table)
    DeleteDups(database, table)
end

println("")
show(dataframe)
println("")

test = ArtistPlaysByTime(database, table, "foo")
histoTimePlot(test, test.time_played)

#topArtists = MostPlayedArtists(database, table, num_entries)
#barPlot(topArtists, topArtists.artist, topArtists.count, 1, num_entries)
#savefig("plots/Top_Artists.pdf")
#
#topSongs = MostPlayedSongs(database, table, num_entries)
#barPlot(topSongs, topSongs.title, topSongs.count, 2, num_entries)
#savefig("plots/Top_Songs.pdf")