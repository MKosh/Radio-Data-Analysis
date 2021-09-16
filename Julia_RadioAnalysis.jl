using HTTP, SQLite, DataFrames, JSONTables, Plots

################################################################################
###
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
###
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
###
function CreateDatabase(DBname::String)
    db = SQLite.DB(DBname)
    return db
end

################################################################################
###
function CreateTable(db, tableName::String)
    command = string("CREATE TABLE IF NOT EXISTS ", tableName, "(artist TEXT, title TEXT,
        date_played TEXT, time_played TEXT, timestamp INTEGER, createdOn TEXT)")
    DBInterface.execute(db, command)
    return nothing
end

################################################################################
###
function DropTable(db, tableName::String)
    command = string("DROP TABLE IF EXISTS ", tableName)
    DBInterface.execute(db, command)
    return nothing
end

################################################################################
###
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
###
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
###
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
###
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
###
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
###
function barPlot(df, x, y, column, limit)
    plotTitle = string("Top ", limit, " ", names(df, column)[1], " Played")
    bar(x, y, xticks=:all, xrotation=45, ylabel="Counts", title=plotTitle,
        legend=false, size=(1000,800))
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

topArtists = MostPlayedArtists(database, table, num_entries)
barPlot(topArtists, topArtists.artist, topArtists.count, 1, num_entries)
savefig("plots/Top_Artists_9_11-9_15.pdf")

topSongs = MostPlayedSongs(database, table, num_entries)
barPlot(topSongs, topSongs.title, topSongs.count, 2, num_entries)
savefig("plots/Top_Songs_9_11-9_15.pdf")