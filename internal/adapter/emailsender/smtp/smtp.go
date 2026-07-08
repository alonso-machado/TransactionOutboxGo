// Package smtp implements domain.EmailSender against a real SMTP server
// using only the Go standard library (net/smtp + mime/multipart) — no
// third-party email SDK dependency. Builds a multipart MIME message with
// the ticket's QR PNG as an attachment, authenticates with PLAIN auth if
// credentials are configured, and sends via net/smtp.SendMail.
package smtp

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"mime/multipart"
	"net"
	"net/smtp"
	"net/textproto"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

type Sender struct {
	host      string
	port      int
	username  string
	password  string
	fromEmail string
	fromName  string
}

func New(host string, port int, username, password, fromEmail, fromName string) *Sender {
	return &Sender{
		host:      host,
		port:      port,
		username:  username,
		password:  password,
		fromEmail: fromEmail,
		fromName:  fromName,
	}
}

func (s *Sender) Send(req domain.EmailRequest) (*domain.EmailResult, error) {
	msg, boundary, err := buildMessage(s.fromEmail, s.fromName, req)
	if err != nil {
		return nil, fmt.Errorf("build mime message: %w", err)
	}

	addr := net.JoinHostPort(s.host, fmt.Sprintf("%d", s.port))
	var auth smtp.Auth
	if s.username != "" {
		auth = smtp.PlainAuth("", s.username, s.password, s.host)
	}

	if err := sendMail(addr, s.host, auth, s.fromEmail, []string{req.ToEmail}, msg); err != nil {
		return nil, fmt.Errorf("send smtp message: %w", err)
	}

	return &domain.EmailResult{ProviderMessageID: boundary}, nil
}

// sendMail mirrors net/smtp.SendMail but upgrades to STARTTLS when the
// server advertises it — net/smtp.SendMail itself never attempts STARTTLS.
func sendMail(addr, host string, auth smtp.Auth, from string, to []string, msg []byte) error {
	c, err := smtp.Dial(addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = c.Close() }()

	if ok, _ := c.Extension("STARTTLS"); ok {
		if err := c.StartTLS(&tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}); err != nil {
			return fmt.Errorf("starttls: %w", err)
		}
	}

	if auth != nil {
		if ok, _ := c.Extension("AUTH"); ok {
			if err := c.Auth(auth); err != nil {
				return fmt.Errorf("auth: %w", err)
			}
		}
	}

	if err := c.Mail(from); err != nil {
		return fmt.Errorf("mail from: %w", err)
	}
	for _, rcpt := range to {
		if err := c.Rcpt(rcpt); err != nil {
			return fmt.Errorf("rcpt to %s: %w", rcpt, err)
		}
	}

	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("data: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close body: %w", err)
	}
	return c.Quit()
}

// buildMessage renders req as a multipart/mixed MIME message: a text/plain
// body part plus one attachment part (base64-encoded, per RFC 2045).
func buildMessage(fromEmail, fromName string, req domain.EmailRequest) ([]byte, string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	headers := fmt.Sprintf("From: %s <%s>\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=%q\r\n\r\n",
		fromName, fromEmail, req.ToEmail, req.Subject, w.Boundary())
	buf.WriteString(headers)

	textPart, err := w.CreatePart(textproto.MIMEHeader{
		"Content-Type": {"text/plain; charset=utf-8"},
	})
	if err != nil {
		return nil, "", err
	}
	if _, err := textPart.Write([]byte(req.BodyText)); err != nil {
		return nil, "", err
	}

	if len(req.Attachment) > 0 {
		attachmentPart, err := w.CreatePart(textproto.MIMEHeader{
			"Content-Type":              {req.AttachmentContentType},
			"Content-Transfer-Encoding": {"base64"},
			"Content-Disposition":       {fmt.Sprintf("attachment; filename=%q", req.AttachmentName)},
		})
		if err != nil {
			return nil, "", err
		}
		encoded := base64.StdEncoding.EncodeToString(req.Attachment)
		if _, err := attachmentPart.Write([]byte(encoded)); err != nil {
			return nil, "", err
		}
	}

	boundary := w.Boundary()
	if err := w.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), boundary, nil
}
