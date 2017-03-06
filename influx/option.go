package influx

type ClientFunc func(c *Client)

func Credentials(addr, username, password string) ClientFunc {
	return func(c *Client) {
		c.addr = addr
		c.username = username
		c.password = password
	}
}

func Database(database string) ClientFunc {
	return func(c *Client) {
		c.database = database
	}
}

func Tags(tags map[string]string) ClientFunc {
	return func(c *Client) {
		c.tags = tags
	}
}
