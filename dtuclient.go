package modbus

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// DTUClientHandler  implements Packager and Transporter interface.
type DTUClientHandler struct {
	rtuPackager
	dtuTransporter
}

// NewDTUClientHandler  allocates and initializes a DTUClientHandler .
func NewDTUClientHandler(dtuID string, conn net.Conn) *DTUClientHandler {
	handler := &DTUClientHandler{}
	handler.dtuID = dtuID
	handler.conn = conn
	handler.Timeout = serialTimeout
	handler.IdleTimeout = serialIdleTimeout
	return handler
}

// DTUClient creates RTU client with default handler and given connect string.
func DTUClient(dtuID string, conn net.Conn) Client {
	handler := NewDTUClientHandler(dtuID, conn)
	return NewClient(handler)
}

// dtuTransporter implements Transporter interface.
type dtuTransporter struct {
	dtuID string
	// Connect & Read timeout
	Timeout time.Duration
	// Idle timeout to close the connection
	IdleTimeout time.Duration
	// Transmission logger
	Logger *log.Logger

	// TCP connection
	mu           sync.Mutex
	conn         net.Conn
	closeTimer   *time.Timer
	lastActivity time.Time
}

func (mb *dtuTransporter) SetTimeout(timeout time.Duration) {
	mb.Timeout = timeout
}

// Send sends data to dtu client and read response.
func (mb *dtuTransporter) Send(aduRequest []byte) (aduResponse []byte, err error) {
	// Establish a new connection if not connected
	if err = mb.connect(); err != nil {
		return
	}
	// Start the timer to close when idle
	mb.lastActivity = time.Now()
	mb.startCloseTimer()

	// Set write and read timeout
	var timeout time.Time
	if mb.Timeout > 0 {
		timeout = mb.lastActivity.Add(mb.Timeout)
	}
	if err = mb.conn.SetDeadline(timeout); err != nil {
		return
	}

	// Send the request
	mb.logf("modbus: sending % x", aduRequest)
	if _, err = mb.conn.Write(aduRequest); err != nil {
		mb.close()
		return
	}

	function := aduRequest[1]
	functionFail := aduRequest[1] & 0x80
	bytesToRead := calculateResponseLength(aduRequest)
	time.Sleep(50 * time.Millisecond)

	var n int
	var n1 int
	var data [rtuMaxSize]byte
	//We first read the minimum length and then read either the full package
	//or the error package, depending on the error status (byte 2 of the response)
	n, err = io.ReadAtLeast(mb.conn, data[:], rtuMinSize)
	if err != nil {
		return
	}
	//if the function is correct
	if data[1] == function {
		//we read the rest of the bytes
		if n < bytesToRead {
			if bytesToRead > rtuMinSize && bytesToRead <= rtuMaxSize {
				if bytesToRead > n {
					n1, err = io.ReadFull(mb.conn, data[n:bytesToRead])
					n += n1
				}
			}
		}
	} else if data[1] == functionFail {
		//for error we need to read 5 bytes
		if n < bytesToRead {
			n1, err = io.ReadFull(mb.conn, data[n:5])
		}
		n += n1
	}

	if err != nil {
		return
	}
	aduResponse = data[:n]
	mb.logf("modbus: received % x\n", aduResponse)
	return
}

// Connect establishes a new connection to the address in Address.
// Connect and Close are exported so that multiple requests can be done with one session
func (mb *dtuTransporter) Connect() error {
	// mb.mu.Lock()
	// defer mb.mu.Unlock()

	return mb.connect()
}

func (mb *dtuTransporter) IsConnected() bool {
	return mb.conn != nil
}

func (mb *dtuTransporter) ChangeConn(conn net.Conn) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.conn != nil {
		mb.conn.Close()
	}

	mb.conn = conn
	return nil
}

func (mb *dtuTransporter) connect() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.conn == nil {
		return errors.New("dtu not connected")
	}
	return nil
}

func (mb *dtuTransporter) startCloseTimer() {
	if mb.IdleTimeout <= 0 {
		return
	}
	if mb.closeTimer == nil {
		mb.closeTimer = time.AfterFunc(mb.IdleTimeout, mb.closeIdle)
	} else {
		mb.closeTimer.Reset(mb.IdleTimeout)
	}
}

// Close closes current connection.
func (mb *dtuTransporter) Close() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	return mb.close()
}

// flush flushes pending data in the connection,
// returns io.EOF if connection is closed.
func (mb *dtuTransporter) flush(b []byte) (err error) {
	if err = mb.conn.SetReadDeadline(time.Now()); err != nil {
		return
	}
	// Timeout setting will be reset when reading
	if _, err = mb.conn.Read(b); err != nil {
		// Ignore timeout error
		if netError, ok := err.(net.Error); ok && netError.Timeout() {
			err = nil
		}
	}
	return
}

func (mb *dtuTransporter) logf(format string, v ...interface{}) {
	if mb.Logger != nil {
		mb.Logger.Printf(format, v...)
	}
}

// closeLocked closes current connection. Caller must hold the mutex before calling this method.
func (mb *dtuTransporter) close() (err error) {
	if mb.conn != nil {
		err = mb.conn.Close()
		mb.conn = nil
	}
	return
}

// closeIdle closes the connection if last activity is passed behind IdleTimeout.
func (mb *dtuTransporter) closeIdle() {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.IdleTimeout <= 0 {
		return
	}
	idle := time.Now().Sub(mb.lastActivity)
	if idle >= mb.IdleTimeout {
		mb.logf("modbus: closing connection due to idle timeout: %v", idle)
		mb.close()
	}
}
