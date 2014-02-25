require 'rubygems'
require 'aws-sdk'
require 'tweetstream'

CONSUMER_KEY        = 'MJw4AQ9ICQhnc6YM6ZNGMg'
CONSUMER_SECRET     = '86hQDp5UKa0OK2hYB3GDYGZdU6Z9QmdyBQvgj1t2nw'
OAUTH_TOKEN         = '191736984-991O0iEzxsO5yYNEJ5aD8GGLwqEUMrGa8Htwhayy'
OAUTH_TOKEN_SECRET  = 'utd50P1tWPuhaUALPiDIhwOFqj2BA5CCm1cEOTrPu0DbF'
TWEET_LANGUAGES     = ['us','de']
TWEET_KEYWORDS      = ['AWS','Kinesis','Sochi','Olympia','Amazon']
TweetStream.configure do |config|
  config.consumer_key       = CONSUMER_KEY
  config.consumer_secret    = CONSUMER_SECRET
  config.oauth_token        = OAUTH_TOKEN
  config.oauth_token_secret = OAUTH_TOKEN_SECRET
  config.auth_method        = :oauth
end

puts "streaming: "
k = AWS::Kinesis.new.client

TweetStream::Client.new.track(TWEET_KEYWORDS) do |status|
  if TWEET_LANGUAGES.include?(status.lang)
    puts "Text: #{status.text}"
    puts "Lang: #{status.lang}"
    puts "Name: #{status.user.screen_name}"
    res = k.put_record(:stream_name => 'tweets', :data => status.text, :partition_key => status.user.screen_name)    
    puts "Put record to Kinesis. Shard: #{res[:shard_id]} - sequence number: #{res[:sequence_number]}"
  end
end
