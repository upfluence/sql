package sqlparser

import (
	"testing"
)

func Test_sqlParser_GetStatementType(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		want StmtType
	}{
		{
			name: "other",
			stmt: "  other stmt",
			want: StmtUnknown,
		},

		{
			name: "select",
			stmt: "(select foo...",
			want: StmtSelect,
		},

		{
			name: "insert",
			stmt: "\nINSERT INTO ...",
			want: StmtInsert,
		},

		{
			name: "update",
			stmt: "UpDaTe bla",
			want: StmtUpdate,
		},

		{
			name: "delete",
			stmt: "\n\nDELETE FROM xx",
			want: StmtDelete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := sqlParser{}
			if got := s.GetStatementType(tt.stmt); got != tt.want {
				t.Errorf("sqlParser.GetStatementType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsDML(t *testing.T) {
	tests := []struct {
		name string
		t    StmtType
		want bool
	}{
		{name: "select", t: StmtSelect},
		{name: "insert", t: StmtInsert, want: true},
		{name: "unknown", t: StmtUnknown, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDML(tt.t); got != tt.want {
				t.Errorf("IsDML() = %v, want %v", got, tt.want)
			}
		})
	}
}
