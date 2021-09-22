using Dates
using HTTP, SQLite, DataFrames, JSONTables, Plots

################################################################################
### DataFrame: Download new data or open old database to create the dataframe
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
### DataFrame: Remove unnecessary information and separate the DateTime into Date & Time
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
### SQL: Open/Create the database
function CreateDatabase(DBname::String)
    db = SQLite.DB(DBname)
    return db
end

################################################################################
### SQL: Create a table if it doesn't already exit
function CreateTable(db, table_name::String)
    command = string("CREATE TABLE IF NOT EXISTS ", table_name, "(artist TEXT, title TEXT,
        date_played TEXT, time_played TEXT, timestamp INTEGER, createdOn TEXT)")
    DBInterface.execute(db, command)
    return nothing
end

################################################################################
### SQL: Remove a table from the datebase
function DropTable(db, table_name::String)
    command = string("DROP TABLE IF EXISTS ", table_name)
    DBInterface.execute(db, command)
    return nothing
end

################################################################################
### SQL: Fill the database from the dataframe
function FillDatabase(df, db, table_name::String)
    for i in 1:nrow(df)
        artist_i = replace(df.artist[i], "'" => "")
        title_i = replace(df.title[i], "'" => "")
        command = string("INSERT INTO ", table_name, " VALUES(", "'",artist_i, "','", 
            title_i, "','", df.date_played[i], "','", df.time_played[i], "',", 
            df.timestamp[i], ",'", df.createdOn[i], "')")
        DBInterface.execute(db, command)
    end
    return nothing
end

################################################################################
### SQL: Return an alphabetical list of artists, their songs, and how often each song was played
function CountPlays(db, table_name::String)
    command = string("SELECT",
                        " artist,",
                        " title,",
                        " COUNT(*) AS count",
                    " FROM",
                        " ", table_name,
                    " GROUP BY",
                        " artist,",
                        " title")
    play_counts = DBInterface.execute(db, command) |> DataFrame
    show(play_counts)
    println("")
    return play_counts
end

################################################################################
### SQL:
function MostPlayedSongs(db, table_name::String, limit)
    command = string("SELECT",
                        " artist,",
                        " title,",
                        " COUNT(*) AS count",
                    " FROM",
                        " ", table_name,
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
function MostPlayedArtists(db, table_name::String, limit)
    command = string("SELECT",
                        " artist,",
                        " COUNT(*) AS count",
                    " FROM",
                        " ", table_name,
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
function DeleteDups(db, table_name::String)
    command = string("DELETE",
                    " FROM",
                        " tracks",
                    " WHERE",
                        " rowid NOT IN (",
                            " SELECT min(rowid)",
                            " FROM",
                                " ", table_name,
                            " GROUP BY",
                                " artist,",
                                " title,",
                                " timestamp)")
    DBInterface.execute(db, command)
    return nothing
end

################################################################################
### SQL: Get times played for a band
function ArtistPlaysByTime(db, table_name::String, Artist::String)
    command = string("SELECT",
                        " time_played,",
                        " COUNT (*) AS count",
                    " FROM",
                        " ", table_name,
                    " WHERE",
                        " artist='", Artist, "'",
                    " GROUP BY",
                        " time_played")
    df = DBInterface.execute(db, command) |> DataFrame
    return df
end

################################################################################
### SQL:
function SongPlaysByTime(db, table_name::String, Artist::String, Song::String)
    command = string("SELECT",
                        " time_played",
                    " FROM",
                        " ", table_name,
                    " WHERE",
                        " artist='", Artist, "'",
                        " AND title ='", Song,"'",
                    " ORDER BY",
                        " time_played")
    df = DBInterface.execute(db, command) |> DataFrame
    return df
end

################################################################################
### SQL: Get song plays for a band
function SongPlaysByArtist(db, table_name::String, Artist::String)
    command = string("SELECT",
                        " title,",
                        " COUNT (*) AS count",
                    " FROM",
                        " ", table_name,
                    " WHERE",
                        " artist='", Artist, "'",
                    " GROUP BY",
                        " title",
                    " ORDER BY",
                        " title")
    df = DBInterface.execute(db, command) |> DataFrame
    return df
end

################################################################################
### Plots: 
function barPlot(df, x, y, column, limit)
    plot_title = string("Top ", limit, " ", names(df, column)[1], " Played")
    println("\n", "Plots", " "^6, ": Plotting ", plotTitle)
    bar_plt = bar(x, y, xticks=:all, xrotation=45, ylabel="Counts", title=plot_title,
        legend=false, size=(1000,800))
    gui(bar_plt)
    return bar_plt
end

################################################################################
### Plots:  86399000000000, 88199000000000,89999000000000
function histoTimePlot(df, x, name::String)
    plot_title = string("What time ", name, " is played")
    tick_marks = ["$i:00" for i in 0:23]
    time_hist = histogram(Dates.Time.(x, "HH:MM:SS.sss"), bins=0:3.6e12:9.0e13, xrotation=45, 
        xlims=(0, 8.7e13), xticks=(0:3.6e12:9.0e13,tick_marks), legend=false, title=plot_title)
    gui(time_hist)
    return time_hist
end

################################################################################
###

database_file   = "radio.db"
table          = "tracks"
num_entries    = 25
getting_new_data = true

if (isfile(database_file) && !getting_new_data)
    println("\n","SQL"," "^8, ": Opening Database: ", database_file)
    database = CreateDatabase(database_file)
    println(" "^11, ": Creating table: ", table, " if it doesn't already exist")
    CreateTable(database, table)
    println("DataFrame"," "^2, ": Creating a dataframe from ", database_file)
    dataframe = InitializeDF(false, database_file, table)
elseif (getting_new_data)
    println("\n", "DataFrame", " "^2, ": Pulling data and initializing the dataframe")
    dataframe = InitializeDF(true)
    InitialDFClean(dataframe)
    println("SQL"," "^8, ": Opening Database: ", database_file)
    database = CreateDatabase(database_file)
    println(" "^11, ": Creating table: ", table, " if it doesn't already exist")
    CreateTable(database, table)
    println(" "^11, ": Filling the database from the dataframe and removing dups")
    FillDatabase(dataframe, database, table)
    DeleteDups(database, table)
end

println("")
show(DBInterface.execute(database, "SELECT COUNT(*) AS data_points FROM tracks") |> DataFrame)
println("")

test = SongPlaysByTime(database, table, "Foo Fighters", "Making a Fire")
test2 = histoTimePlot(test, test.time_played, "Foo Fighters - Making a fire")

#topArtists = MostPlayedArtists(database, table, num_entries)
#barPlot(topArtists, topArtists.artist, topArtists.count, 1, num_entries)
#savefig("plots/Top_Artists.pdf")
#
#topSongs = MostPlayedSongs(database, table, num_entries)
#barPlot(topSongs, topSongs.title, topSongs.count, 2, num_entries)
#savefig("plots/Top_Songs.pdf")