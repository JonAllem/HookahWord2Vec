<Query Kind="Program">
  <Connection>
    <ID>39c8f572-1885-4a6a-b64a-9b1bdd1056c3</ID>
    <Persist>true</Persist>
    <Driver Assembly="IQDriver" PublicKeyToken="5b59726538a49684">IQDriver.IQDriver</Driver>
    <Provider>Devart.Data.MySql</Provider>
    <CustomCxString>AQAAANCMnd8BFdERjHoAwE/Cl+sBAAAAQSqtpEqMw0qiFVkhIKMGggAAAAACAAAAAAAQZgAAAAEAACAAAAAMHg8SFz046w+1rWnM7AZgputMr7ZETyKcYSYl2xKxkgAAAAAOgAAAAAIAACAAAAD9+QNE/QrKidlI4rI+8Tte6GnxuDiVL9NFNOry6XGYgXAAAACckM7o06+Gm0zsTDH3SMO+0ScylgBaQ70Z2ql28AD1UlCNpIlrsSJ4/1Ur89910wkkQQJVjytqRvTNpau79XAMQe+GQ6sxt8z/zRY0M2+gDFpATJERiJxNrpRdgGKUooeSJ6i/KxZ5z4PzyzIwRuHaQAAAAIkTv0YPxfdenDFXEn4fVtDrU2zNV5t9DakoK8y78iiKqYngO6vHambrSYZwSlS2FLA4/6x3y/adcJxBFaRD7YU=</CustomCxString>
    <DriverData>
      <StripUnderscores>false</StripUnderscores>
      <QuietenAllCaps>false</QuietenAllCaps>
      <ExtraCxOptions>CharSet=utf8mb4</ExtraCxOptions>
    </DriverData>
  </Connection>
</Query>

void Main()
{
	var dateRanges = new List<(DateTime Start, DateTime Stop)> {
//		(Start: new DateTime(2017, 4, 1), Stop: new DateTime(2017, 5, 1)),
//		(Start: new DateTime(2017, 5, 1), Stop: new DateTime(2017, 6, 1)),
//		(Start: new DateTime(2017, 6, 1), Stop: new DateTime(2017, 7, 1)),
//		(Start: new DateTime(2017, 7, 1), Stop: new DateTime(2017, 8, 1)),
//		(Start: new DateTime(2017, 8, 1), Stop: new DateTime(2017, 9, 1)),
//		(Start: new DateTime(2017, 9, 1), Stop: new DateTime(2017, 10, 1)),
//		(Start: new DateTime(2017, 10, 1), Stop: new DateTime(2017, 11, 1)),
//		(Start: new DateTime(2017, 11, 1), Stop: new DateTime(2017, 12, 1)),
		(Start: new DateTime(2017, 12, 1), Stop: new DateTime(2018, 1, 1)),
		(Start: new DateTime(2018, 1, 1), Stop: new DateTime(2018, 2, 1)),
		(Start: new DateTime(2018, 2, 1), Stop: new DateTime(2018, 3, 1)),
		(Start: new DateTime(2018, 3, 1), Stop: new DateTime(2018, 4, 1))
	};

//	foreach (var (start, stop) in dateRanges)
//	{
//		var filename = $@"D:\Documents\HSC\Network Analysis\Data\Tweets-{start.Month}-{start.Year}.csv";
//		Util.WriteCsv(Concatenate(GetTweets(start, stop)), filename);
//		Console.WriteLine("Finished 1 tweet query.");
//	}

	foreach (var (start, stop) in dateRanges)
	{
		var filename = $@"D:\Documents\HSC\Network Analysis\Data\Raw\Users-{start.Month}-{start.Year}.csv";
		Util.WriteCsv(Concatenate(GetUsers(start, stop)), filename);
		Console.WriteLine("Finished 1 user query.");
	}

	var files = Directory.GetFiles(@"D:\Documents\HSC\Network Analysis\Data\Raw\", "Users-*.csv");
	var userDict = new Dictionary<string, string>();
	foreach (var user in files.SelectMany(x => MyExtensions.ReadCsv(x).Skip(1)))
	{
		var (id, screenName) = (user[0], user[2]);
		if (!userDict.ContainsKey(id))
		{
			userDict[id] = screenName;
		}
	}
	Util.WriteCsv(userDict, @"D:\Documents\HSC\Network Analysis\Data\Raw\AllUsers.csv");
}

// Define other methods and classes here
public IEnumerable<IQueryable<Tweet>> GetTweets(DateTime startDate, DateTime endDate, int? limit = null)
{
	if (startDate > endDate)
	{
		throw new ArgumentException("startDate should be before endDate!");
	}
	var tweets = Tweets.Where(x => x.CreatedAt > startDate && x.CreatedAt < endDate)
				.Select(x => new Tweet {Id=x.Id, CreatedAt=x.CreatedAt, Text=x.Text, UserId=x.UserId, IsRetweet=x.IsRetweet});
	if (limit.HasValue)
	{
		tweets = tweets.Take(limit.Value);
	}
	if (startDate.Year >= 2018 || (startDate.Year == 2017 && startDate.Month == 12))
	{
		yield return tweets;
	}
	if (startDate.Year < 2018)
	{
		var tweets2 = Tweets255s.Where(x => x.CreatedAt > startDate && x.CreatedAt < endDate)
				.Select(x => new Tweet {Id=x.Id, CreatedAt=x.CreatedAt, Text=x.Text, UserId=x.UserId, IsRetweet=x.IsRetweet});
		if (limit.HasValue)
		{
			tweets2 = tweets2.Take(limit.Value);
		}
		yield return tweets2;
	}
}

public IEnumerable<IQueryable<User>> GetUsers(DateTime startDate, DateTime endDate, int? limit = null)
{
	const int divisor = 30;
	var daysBetween = (endDate - startDate).TotalDays / divisor;
	var tweetQueries = Enumerable.Range(1, divisor + 1)
		.SelectMany(i => GetTweets(startDate.AddDays(daysBetween * (i - 1)), startDate.AddDays(daysBetween * i), limit));
	var userQueries = tweetQueries.Select(q => q.Select(x => x.UserId).Distinct()
					.Join(Twitter_profiles, x => x, y => y.UserId, (x, y) => new User {
						UserId = x, Name = y.Name, ScreenName = y.Screenname,
						FriendsCount = y.FriendsCount, FollowersCount = y.FollowersCount,
						Description = y.Description
					})
				);
	return userQueries;
}

public static IEnumerable<T> Concatenate<T>(IEnumerable<IEnumerable<T>> lists)
{
    return lists.SelectMany(x => x);
}

public class Tweet
{
	public string Id;
	public DateTime? CreatedAt;
	public string Text;
	public string UserId;
	public sbyte? IsRetweet;
}

public class User
{
	public string UserId;
	public string Name;
	public string ScreenName;
	public int? FriendsCount;
	public int? FollowersCount;
	public string Description;
}