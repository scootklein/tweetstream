require 'uri'
require 'cgi'
require 'eventmachine'
require 'twitter/json_stream'
require 'multi_json'
require 'json'

module TweetStream
  # Provides simple access to the Twitter Streaming API (http://apiwiki.twitter.com/Streaming-API-Documentation)
  # for Ruby scripts that need to create a long connection to
  # Twitter for tracking and other purposes.
  #
  # Basic usage of the library is to call one of the provided
  # methods and provide proc handlers that will perform actions on
  # a yielded TweetStream::Status. For example:
  #
  #     @client = TweetStream::Client.new(auth)
  #     @client.on_status { |status|
  #       puts "[#{status.user.screen_name}] #{status.text}"
  #     end
  #     @client.track('fail')
  #
  # For information about a daemonized TweetStream client,
  # view the TweetStream::Daemon class.
  class Client
    attr_reader :parser, :auth

    # Set the JSON Parser for this client. Acceptable options are:
    #
    # <tt>:json_gem</tt>:: Parse using the JSON gem.
    # <tt>:json_pure</tt>:: Parse using the pure-ruby implementation of the JSON gem.
    # <tt>:active_support</tt>:: Parse using ActiveSupport::JSON.decode
    # <tt>:yajl</tt>:: Parse using <tt>yajl-ruby</tt>.
    #
    # You may also pass a class that will return a hash with symbolized
    # keys when <tt>YourClass.parse</tt> is called with a JSON string.
    def parser=(parser)
      @parser = parser_from(parser)
    end
    
    # Set the authentication credentials for this client. Expects one of:
    # 
    # <tt>basic auth</tt>:: Hash with keys of username and password
    # <tt>oauth</tt>:: Hash with keys of consumer_key, consumer_secret, access_key, and access_secret
    # 
    # Raises ArgumentError if the auth hash cannot be validated
    # (doesn't check for proper access, just for valid format)
    def auth=(auth)
      @auth = validate_auth(auth)
    end

    # Create a new client with the Twitter credentials
    # of the account you want to be using its API quota.
    # You may also set the JSON parsing library as specified
    # in the #parser= setter.
    def initialize(auth, parser = :json_gem)      
      self.auth = auth
      self.parser = parser
    end

    # Returns all public statuses. The Firehose is not a generally
    # available resource. Few applications require this level of access.
    # Creative use of a combination of other resources and various access
    # levels can satisfy nearly every application use case.
    def firehose(query_parameters = {})
      start('statuses/firehose', query_parameters)
    end

    # Returns all retweets. The retweet stream is not a generally available
    # resource. Few applications require this level of access. Creative
    # use of a combination of other resources and various access levels
    # can satisfy nearly every application use case. As of 9/11/2009,
    # the site-wide retweet feature has not yet launched,
    # so there are currently few, if any, retweets on this stream.
    def retweet(query_parameters = {})
      start('statuses/retweet', query_parameters)
    end

    # Returns a random sample of all public statuses. The default access level
    # provides a small proportion of the Firehose. The "Gardenhose" access
    # level provides a proportion more suitable for data mining and
    # research applications that desire a larger proportion to be statistically
    # significant sample.
    def sample(query_parameters = {})
      start('statuses/sample', query_parameters)
    end

    # Specify keywords to track. Queries are subject to Track Limitations,
    # described in Track Limiting and subject to access roles, described in
    # the statuses/filter method. Track keywords are case-insensitive logical
    # ORs. Terms are exact-matched, and also exact-matched ignoring
    # punctuation. Phrases, keywords with spaces, are not supported.
    # Keywords containing punctuation will only exact match tokens.
    # Query parameters may be passed as the last argument.
    def track(*keywords)
      query_params = keywords.pop if keywords.last.is_a?(::Hash)
      query_params ||= {}
      filter(query_params.merge(:track => keywords))
    end

    # Returns public statuses from or in reply to a set of users. Mentions
    # ("Hello @user!") and implicit replies ("@user Hello!" created without
    # pressing the reply "swoosh") are not matched. Requires integer user
    # IDs, not screen names. Query parameters may be passed as the last argument.
    def follow(*user_ids)
      query_params = user_ids.pop if user_ids.last.is_a?(::Hash)
      query_params ||= {}
      filter(query_params.merge(:follow => user_ids))
    end

    # Specifies a set of bounding boxes to track. Only tweets that are both created
    # using the Geotagging API and are placed from within a tracked bounding box will
    # be included in the stream – the user’s location field is not used to filter tweets
    # (e.g. if a user has their location set to “San Francisco”, but the tweet was not created
    # using the Geotagging API and has no geo element, it will not be included in the stream).
    # Bounding boxes are specified as a comma separate list of longitude/latitude pairs, with
    # the first pair denoting the southwest corner of the box
    # longitude/latitude pairs, separated by commas. The first pair specifies the southwest corner of the box.
    def locations(*locations_map)
        query_params = locations_map.pop if locations_map.last.is_a?(::Hash)
        query_params ||= {}
        filter(query_params.merge(:locations => locations_map))
    end

    # Make a call to the statuses/filter method of the Streaming API,
    # you may provide <tt>:follow</tt>, <tt>:track</tt> or both as options
    # to follow the tweets of specified users or track keywords. This
    # method is provided separately for cases when it would conserve the
    # number of HTTP connections to combine track and follow.
    def filter(query_params = {})
      [:follow, :track, :locations].each do |param|
        if query_params[param].is_a?(Array)
          query_params[param] = query_params[param].flatten.collect{|q| q.to_s}.join(',')
        elsif query_params[param]
          query_params[param] = query_params[param].to_s
        end
      end
      start('statuses/filter', query_params.merge(:method => :post))
    end
    
    # Make a call to the user of the userstream API,
    # you may provide :all_replies set to 1 to receive all activity
    # directed toward all of those that the oauth user follows
    # (this behavior usually only happens when both parties are followed)
    def user(query_params = {})
      raise ArgumentError, "user method only allowed with OAuth" unless self.auth[:type] == :oauth
      start('user', query_params)
    end
    
    # Set a Proc to be run when a status is received
    # from the Twitter stream. For example:
    #
    #     @client = TweetStream::Client.new(auth)
    #     @client.on_status do |status|
    #       puts "#{status.user.screen_name} -- #{status.text}"
    #     end
    #
    def on_status(&block)
      if block_given?
        @on_status = block
        self
      else
        @on_status
      end
    end
    
    # Set a Proc to be run when a direct message is received
    # from the Twitter stream. For example:
    #
    #     @client = TweetStream::Client.new(auth)
    #     @client.on_direct_message do |status|
    #       puts "#{status.user.screen_name} -- #{status.text}"
    #     end
    #
    def on_direct_message(&block)
      if block_given?
        @on_direct_message = block
        self
      else
        @on_direct_message
      end
    end
    
    # Set a Proc to be run when an event is received
    # from the Twitter stream. For example:
    #
    #     @client = TweetStream::Client.new(auth)
    #     @client.on_event do |status|
    #       puts "#{status.user.screen_name} -- #{status.text}"
    #     end
    #
    def on_event(&block)
      if block_given?
        @on_event = block
        self
      else
        @on_event
      end
    end
    
    # Set a Proc to be run when a list of friends is
    # received from the Twitter stream. For example:
    #
    #     @client = TweetStream::Client.new(auth)
    #     @client.on_friends_list do |status|
    #       puts "#{status.user.screen_name} -- #{status.text}"
    #     end
    #
    def on_friends_list(&block)
      if block_given?
        @on_friends_list = block
        self
      else
        @on_friends_list
      end
    end

    # Set a Proc to be run when a deletion notice is received
    # from the Twitter stream. For example:
    #
    #     @client = TweetStream::Client.new(auth)
    #     @client.on_delete do |status_id, user_id|
    #       Tweet.delete(status_id)
    #     end
    #
    # Block must take two arguments: the status id and the user id.
    # If no block is given, it will return the currently set
    # deletion proc. When a block is given, the TweetStream::Client
    # object is returned to allow for chaining.
    def on_delete(&block)
      if block_given?
        @on_delete = block
        self
      else
        @on_delete
      end
    end

    # Set a Proc to be run when a rate limit notice is received
    # from the Twitter stream. For example:
    #
    #     @client = TweetStream::Client.new(auth)
    #     @client.on_limit do |discarded_count|
    #       # Make note of discarded count
    #     end
    #
    # Block must take one argument: the number of discarded tweets.
    # If no block is given, it will return the currently set
    # limit proc. When a block is given, the TweetStream::Client
    # object is returned to allow for chaining.
    def on_limit(&block)
      if block_given?
        @on_limit = block
        self
      else
        @on_limit
      end
    end

    # Set a Proc to be run when an HTTP error is encountered in the
    # processing of the stream. Note that TweetStream will automatically
    # try to reconnect, this is for reference only. Don't panic!
    #
    #     @client = TweetStream::Client.new(auth)
    #     @client.on_error do |message|
    #       # Make note of error message
    #     end
    #
    # Block must take one argument: the error message.
    # If no block is given, it will return the currently set
    # error proc. When a block is given, the TweetStream::Client
    # object is returned to allow for chaining.
    def on_error(&block)
      if block_given?
        @on_error = block
        self
      else
        @on_error
      end
    end

    # Set a Proc to be run when connection established.
    # Called in EventMachine::Connection#post_init
    #
    #     @client = TweetStream::Client.new(auth)
    #     @client.on_inited do
    #       puts 'Connected...'
    #     end
    #
    def on_inited(&block)
      if block_given?
        @on_inited = block
        self
      else
        @on_inited
      end
    end

    def start(path, query_parameters = {}) #:nodoc:
      method = query_parameters.delete(:method) || :get
      delete_proc = query_parameters.delete(:delete) || self.on_delete
      limit_proc = query_parameters.delete(:limit) || self.on_limit
      error_proc = query_parameters.delete(:error) || self.on_error
      status_proc = query_parameters.delete(:status) || self.on_status
      direct_message_proc = query_parameters.delete(:direct_message) || self.on_direct_message
      event_proc = query_parameters.delete(:event) || self.on_event
      friends_list_proc = query_parameters.delete(:friends) || self.on_friends_list
      inited_proc = query_parameters.delete(:inited) || self.on_inited

      uri = method == :get ? build_uri(path, query_parameters) : build_uri(path)
      
      jsonstream_opts = {
        :ssl => use_ssl(path),
        :host => host(path),
        :path => uri,
        :method => method.to_s.upcase,
        :content => (method == :post ? build_post_body(query_parameters) : ''),
        :user_agent => 'TweetStream',
        :on_inited => inited_proc,
        :timeout => 90
      }
      jsonstream_opts[:auth] = "#{auth[:username]}:#{auth[:password]}" if auth[:type] == :basic
      jsonstream_opts[:oauth] = auth if auth[:type] == :oauth # auth hash has all keys and values just like twitter-stream needs them, no change required
      jsonstream_opts[:params] = query_parameters if auth[:type] == :oauth
      
      EventMachine::run {
        @stream = Twitter::JSONStream.connect(jsonstream_opts)

        @stream.each_item do |item|
          raw_hash = @parser.decode(item)

          unless raw_hash.is_a?(::Hash)
            error_proc.call("Unexpected JSON object in stream: #{item}") if error_proc.is_a?(Proc)
            next
          end

          hash = TweetStream::Hash.new(raw_hash)

          if hash[:delete] && hash[:delete][:status]
            delete_proc.call(TweetStream::Delete.new(hash)) if delete_proc.is_a?(Proc)
          elsif hash[:limit] && hash[:limit][:track]
            limit_proc.call(TweetStream::Limit.new(hash)) if limit_proc.is_a?(Proc)
          elsif hash[:friends]
            friends_list_proc.call(TweetStream::FriendsList.new(hash)) if friends_list_proc.is_a?(Proc)
          elsif hash[:text] && hash[:user]
            status_proc.call(TweetStream::Status.new(hash)) if status_proc.is_a?(Proc)
          elsif hash[:direct_message]
            direct_message_proc.call(TweetStream::DirectMessage.new(hash)) if direct_message_proc.is_a?(Proc)
          elsif hash[:event]
            event_proc.call(TweetStream::Event.new(hash)) if event_proc.is_a?(Proc)
          end
        end

        @stream.on_error do |message|
          error_proc.call(message) if error_proc.is_a?(Proc)
        end

        @stream.on_max_reconnects do |timeout, retries|
          raise TweetStream::ReconnectError.new(timeout, retries)
        end
      }
    end

    # Terminate the currently running TweetStream.
    def stop
      EventMachine.stop_event_loop
      @last_status
    end

    protected

    def parser_from(parser)
      MultiJson.engine = parser
      MultiJson
    end
    
    def validate_auth(auth)
      return auth.merge(:type => :basic) if auth[:username] && auth[:password]
      return auth.merge(:type => :oauth) if auth[:consumer_key] && auth[:consumer_secret] && auth[:access_key] && auth[:access_secret]
      
      raise ArgumentError, "Unable to validate auth hash."
    end
    
    def host(path)
      return 'userstream.twitter.com' if path == 'user'
      return 'stream.twitter.com'
    end

    def use_ssl(path)
      return path == 'user'
    end

    def api_ver(path)
      return 2 if path == 'user'
      return 1
    end

    def build_uri(path, query_parameters = {}) #:nodoc:
      uri = "/#{api_ver(path)}/#{path}.json#{build_query_parameters(query_parameters)}"
      #URI.parse(uri)
      uri
    end

    def build_query_parameters(query)
      # oauth query params have to be passed to twitter-stream differently since they're signed
      (query.size > 0 && auth[:type] == :basic) ? "?#{build_post_body(query)}" : ''
    end

    def build_post_body(query) #:nodoc:
      return '' unless query && query.is_a?(::Hash) && query.size > 0
      pairs = []

      query.each_pair do |k,v|
        pairs << "#{k.to_s}=#{CGI.escape(v.to_s)}"
      end

      pairs.join('&')
    end
  end
end
