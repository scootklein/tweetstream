require 'tweetstream/hash' # must be first since other requires inherit from it
require 'tweetstream/delete'
require 'tweetstream/direct_message'
require 'tweetstream/event'
require 'tweetstream/friends_list'
require 'tweetstream/status'
require 'tweetstream/user'
require 'tweetstream/version'
require 'tweetstream/client' # must be last since it uses previous requires
require 'tweetstream/daemon' # must be after client since it inherits from client

module TweetStream
  class Terminated < ::StandardError; end
  class Error < ::StandardError; end
  class ConnectionError < TweetStream::Error; end

  # A ReconnectError is raised when the maximum number of retries has
  # failed to re-establish a connection.
  class ReconnectError < StandardError
    attr_accessor :timeout, :retries
    def initialize(timeout, retries)
      self.timeout = timeout
      self.retries = retries
      super("Failed to reconnect after #{retries} tries.")
    end
  end
end