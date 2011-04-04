class TweetStream::Event < TweetStream::Hash
  def initialize(hash)
    super
    self[:target] = TweetStream::User.new(self[:target])
    self[:source] = TweetStream::User.new(self[:source])
  end
end